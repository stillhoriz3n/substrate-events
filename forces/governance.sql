-- ============================================================
-- THE GOVERNANCE DIAL — one value, 1-100, controls the entire pipe
-- ============================================================

-- Field type: governance — a single integer 1-100 that controls pipe behavior
INSERT INTO substrate.blob (unid, fields, subscriber) VALUES
('00000000-0000-0000-0001-000000000070',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"governance"},"description":{"type":"utf8","value":"Pipe governance intensity 1-100. 1=firehose, 50=balanced, 100=fortress. One number controls rate, gate, dedup, compression, backpressure, circuit breaker, ack, priority."}}',
 '{SYSTEM}');

-- ============================================================
-- FORCE: governance_profile — maps 1-100 to concrete parameters
-- This IS the algorithm. Change this function, change every pipe.
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.governance_profile(p_level INT)
RETURNS JSONB AS $$
import json, math

level = max(1, min(100, p_level))

# --- RATE LIMIT ---
# 1 = unlimited, 100 = 1/hour
if level <= 5:
    rate_limit = None  # no limit
elif level <= 20:
    rate_limit = '1000/hour'
elif level <= 40:
    rate_limit = '100/hour'
elif level <= 60:
    rate_limit = '50/hour'
elif level <= 80:
    rate_limit = '10/hour'
elif level <= 95:
    rate_limit = '5/hour'
else:
    rate_limit = '1/hour'

# --- GATE ---
# Low = no gates, high = block secrets + large blobs + sensitive compositions
gates = []
if level >= 20:
    gates.append('composition!=secret')
if level >= 50:
    # Max size decreases as governance increases
    # 50 = 500MB, 75 = 100MB, 100 = 10MB
    max_mb = max(10, int(500 - (level - 50) * 9.8))
    gates.append(f'size<{max_mb * 1048576}')
if level >= 70:
    gates.append('composition!=credential')
if level >= 90:
    gates.append('composition!=memory')
gate = ','.join(gates) if gates else None

# --- DEDUP ---
# 1 = no dedup, 100 = 24h window
if level <= 10:
    dedup = None
elif level <= 30:
    dedup = 'PT5M'    # 5 minutes
elif level <= 50:
    dedup = 'PT1H'    # 1 hour
elif level <= 70:
    dedup = 'PT6H'    # 6 hours
elif level <= 90:
    dedup = 'PT12H'   # 12 hours
else:
    dedup = 'P1D'     # 24 hours

# --- COMPRESSION ---
# Low = no compression, high = always compress
if level <= 15:
    compress = None
elif level <= 40:
    compress = 'gzip'   # faster, less compression
else:
    compress = 'zlib'   # slower, better compression

# --- BACKPRESSURE ---
# Low = drop (performance), mid = queue (reliability), high = slow (safety)
if level <= 25:
    backpressure = 'drop'
elif level <= 75:
    backpressure = 'queue'
else:
    backpressure = 'slow'

# --- CIRCUIT BREAKER ---
# Low = tolerant (many failures before trip), high = sensitive (few failures)
if level <= 10:
    cb_max_failures = 100  # basically never trips
elif level <= 30:
    cb_max_failures = 20
elif level <= 50:
    cb_max_failures = 10
elif level <= 70:
    cb_max_failures = 5
elif level <= 90:
    cb_max_failures = 3
else:
    cb_max_failures = 1

# Cooldown: low = short recovery, high = long recovery
cb_cooldown = max(30, int(level * 6))  # 6s to 600s (10 min)

# --- ACK REQUIRED ---
# Below 40 = no ack needed, above = must acknowledge
ack_required = level >= 40

# --- PRIORITY ---
# Inverse of governance: low governance = high priority (fast path)
# High governance = low priority (careful path)
priority = max(1, 100 - level)

# --- MAX QUEUE DEPTH ---
# How many emissions can queue before dropping
if level <= 20:
    max_queue = 10
elif level <= 50:
    max_queue = 100
elif level <= 80:
    max_queue = 1000
else:
    max_queue = 10000

profile = {
    'level': level,
    'rate_limit': rate_limit,
    'gate': gate,
    'dedup': dedup,
    'compress': compress,
    'backpressure': backpressure,
    'cb_max_failures': cb_max_failures,
    'cb_cooldown_seconds': cb_cooldown,
    'ack_required': ack_required,
    'priority': priority,
    'max_queue': max_queue
}

return json.dumps(profile)
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: set_governance — apply a governance level to a subscription
-- One call. One number. Every governor field is set.
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.set_governance(
    p_subscription_unid UUID,
    p_level INT
) RETURNS JSONB AS $$
import json

profile_row = plpy.execute(plpy.prepare(
    "SELECT substrate.governance_profile($1) as profile", ["int"]
), [p_level])

profile = json.loads(profile_row[0]['profile'])

# Build the governor fields from the profile
updates = {
    'governance': {'type': 'governance', 'value': p_level}
}

if profile['rate_limit']:
    updates['rate_limit'] = {'type': 'rate_limit', 'value': profile['rate_limit']}
else:
    updates['rate_limit'] = {'type': 'rate_limit', 'value': 'unlimited'}

if profile['gate']:
    updates['gate'] = {'type': 'gate', 'value': profile['gate']}

if profile['dedup']:
    updates['dedup'] = {'type': 'dedup', 'value': profile['dedup']}

if profile['compress']:
    updates['compress'] = {'type': 'algorithm', 'value': profile['compress']}

updates['backpressure'] = {'type': 'utf8', 'value': profile['backpressure']}
updates['ack_required'] = {'type': 'boolean', 'value': profile['ack_required']}
updates['priority'] = {'type': 'integer', 'value': profile['priority']}

# Apply all at once
plan = plpy.prepare("""
    UPDATE substrate.blob SET fields = fields || $1::jsonb
    WHERE unid = $2
""", ["text", "uuid"])
plpy.execute(plan, [json.dumps(updates), p_subscription_unid])

# Also update pipe_state circuit breaker thresholds
ps_plan = plpy.prepare("""
    UPDATE substrate.blob SET fields = fields || jsonb_build_object(
        'cb_max_failures', jsonb_build_object('type', 'integer', 'value', $1),
        'cb_cooldown', jsonb_build_object('type', 'integer', 'value', $2),
        'max_queue', jsonb_build_object('type', 'integer', 'value', $3)
    )
    WHERE fields->'composition'->>'value' = 'pipe_state'
    AND fields->'subscription'->>'value' = $4
""", ["int", "int", "int", "text"])
plpy.execute(ps_plan, [
    profile['cb_max_failures'],
    profile['cb_cooldown_seconds'],
    profile['max_queue'],
    str(p_subscription_unid)
])

# Signal
sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'set_governance', $2::jsonb,
            '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text"])
plpy.execute(sig, [p_subscription_unid, json.dumps(profile)])

return json.dumps(profile)
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: governed_propagate_v2 — uses the governance dial
-- Reads the governance level from the subscription, derives
-- all parameters from it, runs the full chain.
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.governed_propagate_v2(p_blob_unid UUID)
RETURNS JSONB AS $$
import json

blob_row = plpy.execute(plpy.prepare(
    "SELECT fields, content_hash FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_blob_unid])

if not blob_row:
    return json.dumps({'error': 'blob not found'})

blob_fields = blob_row[0]['fields']
if isinstance(blob_fields, str):
    blob_fields = json.loads(blob_fields)

content_hash = blob_row[0]['content_hash'] or ''
blob_composition = blob_fields.get('composition', {}).get('value', '')
blob_name = blob_fields.get('name', {}).get('value', '')

subs = plpy.execute("""
    SELECT unid, fields FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'subscription'
    AND (fields->'state' IS NULL OR fields->'state'->>'value' != 'retired')
""")

results = {
    'blob': str(p_blob_unid),
    'composition': blob_composition,
    'name': blob_name,
    'subscriptions_checked': len(subs),
    'gated': 0, 'deduped': 0, 'throttled': 0,
    'emitted': 0, 'queued': 0, 'circuit_broken': 0, 'errors': 0
}

for sub in subs:
    sub_fields = sub['fields']
    if isinstance(sub_fields, str):
        sub_fields = json.loads(sub_fields)

    sub_unid = sub['unid']
    target = sub_fields.get('target', {}).get('value', '')

    # Match filter
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

    # Get governance level — derive all params from one number
    gov_level = int(sub_fields.get('governance', {}).get('value', 50))
    profile_row = plpy.execute(plpy.prepare(
        "SELECT substrate.governance_profile($1) as p", ["int"]
    ), [gov_level])
    profile = json.loads(profile_row[0]['p'])

    # 1. GATE
    if profile['gate']:
        gated = plpy.execute(plpy.prepare(
            "SELECT substrate.gate($1, $2) as ok", ["uuid", "text"]
        ), [p_blob_unid, profile['gate']])
        if not gated[0]['ok']:
            results['gated'] += 1
            continue

    # 2. PIPE STATE
    ps_row = plpy.execute(plpy.prepare(
        "SELECT substrate.ensure_pipe_state($1) as unid", ["uuid"]
    ), [sub_unid])
    pipe_state_unid = ps_row[0]['unid']

    # 3. CIRCUIT BREAKER
    cb_status = plpy.execute(plpy.prepare(
        "SELECT substrate.circuit_breaker($1, $2, $3) as status",
        ["uuid", "int", "int"]
    ), [pipe_state_unid, profile['cb_max_failures'], profile['cb_cooldown_seconds']])
    if cb_status[0]['status'] == 'tripped':
        results['circuit_broken'] += 1
        continue

    # 4. DEDUP
    if profile['dedup']:
        is_new = plpy.execute(plpy.prepare(
            "SELECT substrate.dedup($1, $2, $3) as ok", ["uuid", "text", "text"]
        ), [pipe_state_unid, content_hash, profile['dedup']])
        if not is_new[0]['ok']:
            results['deduped'] += 1
            continue

    # 5. THROTTLE
    allowed = True
    if profile['rate_limit']:
        throttle_r = plpy.execute(plpy.prepare(
            "SELECT substrate.throttle($1, $2) as ok", ["uuid", "text"]
        ), [pipe_state_unid, profile['rate_limit']])
        allowed = throttle_r[0]['ok']

    if not allowed:
        bp = profile['backpressure']
        if bp == 'drop':
            results['throttled'] += 1
            continue
        else:
            plpy.execute(plpy.prepare(
                "SELECT substrate.enqueue($1, $2, $3)", ["uuid", "uuid", "int"]
            ), [sub_unid, p_blob_unid, profile['priority']])
            results['queued'] += 1
            continue

    # 6. COMPRESS + EMIT
    endpoint = sub_fields.get('endpoint', {}).get('value', '')
    protocol = sub_fields.get('protocol', {}).get('value', 'http')

    source_unid = p_blob_unid
    if profile['compress'] and blob_fields.get('content', {}).get('type') != 'compressed':
        try:
            comp_row = plpy.execute(plpy.prepare(
                "SELECT substrate.compress($1, $2) as unid", ["uuid", "text"]
            ), [p_blob_unid, profile['compress']])
            source_unid = comp_row[0]['unid']
        except:
            pass

    try:
        plpy.execute(plpy.prepare(
            "SELECT substrate.emit($1, $2, $3, false)", ["uuid", "text", "text"]
        ), [source_unid, endpoint, protocol])

        plpy.execute(plpy.prepare("""
            UPDATE substrate.blob SET fields = fields
                || jsonb_build_object(
                    'last_emit', jsonb_build_object('type','timestamp','value', now()::text),
                    'failures', jsonb_build_object('type','integer','value', 0)
                )
            WHERE unid = $1
        """, ["uuid"]), [pipe_state_unid])

        if cb_status[0]['status'] == 'half_open':
            plpy.execute(plpy.prepare("""
                UPDATE substrate.blob SET fields = jsonb_set(
                    fields, '{status,value}', '"open"'::jsonb
                ) WHERE unid = $1
            """, ["uuid"]), [pipe_state_unid])

        results['emitted'] += 1
    except Exception as e:
        plpy.execute(plpy.prepare("""
            UPDATE substrate.blob SET fields = jsonb_set(
                fields, '{failures,value}',
                to_jsonb((COALESCE((fields->'failures'->>'value')::int, 0) + 1))
            ) WHERE unid = $1
        """, ["uuid"]), [pipe_state_unid])
        results['errors'] += 1

if results['emitted'] > 0 or results['queued'] > 0:
    sig = plpy.prepare("""
        INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
        VALUES ($1, 'governed_propagate', $2::jsonb,
                '00000000-0000-0000-0000-000000000001')
    """, ["uuid", "text"])
    plpy.execute(sig, [p_blob_unid, json.dumps(results)])

return json.dumps(results)
$$ LANGUAGE plpython3u;
