-- ============================================================
-- SUBSCRIBE AS A FORCE — new field types + composition + forces
-- ============================================================

-- New field types for subscription semantics
INSERT INTO substrate.blob (unid, fields, subscriber) VALUES
('00000000-0000-0000-0001-000000000050', 
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"endpoint"},"description":{"type":"utf8","value":"A reachable address — URL, connstring, path, socket"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000051',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"protocol"},"description":{"type":"utf8","value":"Delivery method — http, pg, ws, s3, grpc, file"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000052',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"frequency"},"description":{"type":"utf8","value":"Delivery trigger — on_change, on_create, once, periodic"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000053',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"filter"},"description":{"type":"utf8","value":"Blob match predicate — composition type, name pattern, field condition"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000054',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"compressed"},"description":{"type":"utf8","value":"Content stored after compression — algorithm in sibling field"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000055',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"algorithm"},"description":{"type":"utf8","value":"Compression algorithm identifier — zlib, gzip, lz4, zstd"}}',
 '{SYSTEM}');

-- New composition: subscription
INSERT INTO substrate.blob (unid, fields, subscriber) VALUES
('00000000-0000-0000-0002-000000000030',
 '{"composition":{"type":"utf8","value":"composition"},"name":{"type":"utf8","value":"subscription"},"description":{"type":"utf8","value":"A delivery relationship — who wants what, where, how, when, whether to compress. Subscribe is a force."}}',
 '{SYSTEM}');

-- ============================================================
-- FORCE: subscribe — create a subscription blob
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.subscribe(
    p_subscriber_name TEXT,          -- who
    p_target_filter TEXT,            -- what (e.g. 'composition=file', '*', 'name=claude.exe')
    p_endpoint TEXT,                 -- where
    p_protocol TEXT DEFAULT 'pg',    -- how
    p_frequency TEXT DEFAULT 'on_change',  -- when
    p_compress TEXT DEFAULT NULL     -- compression algorithm (null = no compression)
) RETURNS UUID AS $$
import json

fields = {
    'composition': {'type': 'utf8', 'value': 'subscription'},
    'subscriber':  {'type': 'utf8', 'value': p_subscriber_name},
    'target':      {'type': 'filter', 'value': p_target_filter},
    'endpoint':    {'type': 'endpoint', 'value': p_endpoint},
    'protocol':    {'type': 'protocol', 'value': p_protocol},
    'frequency':   {'type': 'frequency', 'value': p_frequency},
}

if p_compress:
    fields['compress'] = {'type': 'algorithm', 'value': p_compress}

plan = plpy.prepare("""
    INSERT INTO substrate.blob (fields, subscriber)
    VALUES ($1::jsonb, '{SYSTEM}')
    RETURNING unid
""", ["text"])

row = plpy.execute(plan, [json.dumps(fields)])
sub_unid = row[0]['unid']

sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'subscribe', jsonb_build_object(
        'subscriber', $2, 'target', $3, 'endpoint', $4, 'protocol', $5
    ), '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text", "text", "text", "text"])
plpy.execute(sig, [sub_unid, p_subscriber_name, p_target_filter, p_endpoint, p_protocol])

plpy.notice(f'Subscription created: {p_subscriber_name} -> {p_target_filter} via {p_protocol}://{p_endpoint}')
return sub_unid
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: unsubscribe — retire a subscription blob
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.unsubscribe(p_subscription_unid UUID)
RETURNS VOID AS $$

plan = plpy.prepare("""
    UPDATE substrate.blob 
    SET fields = jsonb_set(fields, '{state}', jsonb_build_object('type', 'utf8', 'value', 'retired'))
    WHERE unid = $1 AND fields->'composition'->>'value' = 'subscription'
""", ["uuid"])
plpy.execute(plan, [p_subscription_unid])

sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'unsubscribe', '{}', '00000000-0000-0000-0000-000000000001')
""", ["uuid"])
plpy.execute(sig, [p_subscription_unid])
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: propagate — fan out blob changes to all matching subscriptions
-- Called by Radiation (Law 5) or manually. Finds all active subscriptions
-- whose filter matches the changed blob, then compresses + emits.
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.propagate(p_blob_unid UUID)
RETURNS INT AS $$
import json

# Get the changed blob's metadata
blob_row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_blob_unid])

if not blob_row:
    return 0

blob_fields = blob_row[0]['fields']
if isinstance(blob_fields, str):
    blob_fields = json.loads(blob_fields)

blob_composition = blob_fields.get('composition', {}).get('value', '')
blob_name = blob_fields.get('name', {}).get('value', '')

# Find all active subscription blobs
subs = plpy.execute("""
    SELECT unid, fields FROM substrate.blob
    WHERE fields->>'composition' IS NOT NULL
    AND fields->'composition'->>'value' = 'subscription'
    AND (fields->'state' IS NULL OR fields->'state'->>'value' != 'retired')
""")

delivered = 0

for sub in subs:
    sub_fields = sub['fields']
    if isinstance(sub_fields, str):
        sub_fields = json.loads(sub_fields)
    
    target = sub_fields.get('target', {}).get('value', '')
    endpoint = sub_fields.get('endpoint', {}).get('value', '')
    protocol = sub_fields.get('protocol', {}).get('value', 'http')
    compress_algo = sub_fields.get('compress', {}).get('value', None)
    
    # Match filter against blob
    match = False
    if target == '*':
        match = True
    elif '=' in target:
        key, val = target.split('=', 1)
        blob_val = blob_fields.get(key, {}).get('value', '')
        match = (str(blob_val) == val)
    elif target == blob_composition:
        match = True
    
    if not match:
        continue
    
    # Deliver: compress if requested, then emit
    source_unid = p_blob_unid
    
    if compress_algo and blob_fields.get('content', {}).get('type') != 'compressed':
        # Compress first
        comp_row = plpy.execute(plpy.prepare(
            "SELECT substrate.compress($1, $2) as unid", ["uuid", "text"]
        ), [p_blob_unid, compress_algo])
        source_unid = comp_row[0]['unid']
    
    # Emit
    plpy.execute(plpy.prepare(
        "SELECT substrate.emit($1, $2, $3, false)", ["uuid", "text", "text"]
    ), [source_unid, endpoint, protocol])
    
    delivered += 1
    plpy.notice(f'Propagated blob {p_blob_unid} to {endpoint} via {protocol}')

# Signal the propagation
if delivered > 0:
    sig = plpy.prepare("""
        INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
        VALUES ($1, 'propagate', jsonb_build_object('delivered_to', $2),
                '00000000-0000-0000-0000-000000000001')
    """, ["uuid", "int"])
    plpy.execute(sig, [p_blob_unid, delivered])

return delivered
$$ LANGUAGE plpython3u;
