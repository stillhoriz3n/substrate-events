CREATE OR REPLACE FUNCTION substrate.propagate(p_blob_unid uuid)
 RETURNS integer
 LANGUAGE plpython3u
AS $function$
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
$function$
