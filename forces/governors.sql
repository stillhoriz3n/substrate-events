-- ============================================================
-- GOVERNOR PRIMITIVES — flow control for the subscription pipe
-- Field types, state tables, and forces that prevent data blasting
-- ============================================================

-- New field types for pipe governance
INSERT INTO substrate.blob (unid, fields, subscriber) VALUES
('00000000-0000-0000-0001-000000000060',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"rate_limit"},"description":{"type":"utf8","value":"Max emissions per time window — 10/minute, 100/hour, 1000/day"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000061',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"max_size"},"description":{"type":"utf8","value":"Maximum blob size in bytes that may traverse this pipe"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000062',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"backpressure"},"description":{"type":"utf8","value":"Behavior when pipe is overwhelmed — drop, queue, slow"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000063',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"ttl"},"description":{"type":"utf8","value":"Time-to-live — duration before auto-retirement. ISO 8601 duration."}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000064',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"gate"},"description":{"type":"utf8","value":"Content filter that restricts what may traverse a pipe — composition!=secret, size<10MB"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000065',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"dedup"},"description":{"type":"utf8","value":"Deduplication window — duration to remember sent content hashes. ISO 8601."}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000066',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"ack_required"},"description":{"type":"utf8","value":"Whether the receiver must acknowledge delivery before the emission is considered complete"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000067',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"priority"},"description":{"type":"utf8","value":"Delivery priority — integer, higher value = delivered first when constrained"}}',
 '{SYSTEM}');

-- New compositions for pipe state
INSERT INTO substrate.blob (unid, fields, subscriber) VALUES
('00000000-0000-0000-0002-000000000031',
 '{"composition":{"type":"utf8","value":"composition"},"name":{"type":"utf8","value":"emission"},"description":{"type":"utf8","value":"A record of a blob traversing a pipe — tracks state from queued through delivered or dropped"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0002-000000000032',
 '{"composition":{"type":"utf8","value":"composition"},"name":{"type":"utf8","value":"pipe_state"},"description":{"type":"utf8","value":"Runtime state of a subscription pipe — rate counters, dedup cache, backpressure status, queue depth"}}',
 '{SYSTEM}');

-- ============================================================
-- PIPE STATE TABLE — tracks rate counters, dedup hashes, queue
-- This is a blob, not a separate table. The OS stores itself.
-- ============================================================

-- Force: ensure_pipe_state — creates or returns the pipe_state blob for a subscription
CREATE OR REPLACE FUNCTION substrate.ensure_pipe_state(p_subscription_unid UUID)
RETURNS UUID AS $$
import json

existing = plpy.execute(plpy.prepare("""
    SELECT unid FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'pipe_state'
    AND fields->'subscription'->>'value' = $1
    AND (fields->'state' IS NULL OR fields->'state'->>'value' != 'retired')
    LIMIT 1
""", ["text"]), [str(p_subscription_unid)])

if existing:
    return existing[0]['unid']

plan = plpy.prepare("""
    INSERT INTO substrate.blob (fields, subscriber)
    VALUES (
        jsonb_build_object(
            'composition',   jsonb_build_object('type', 'utf8', 'value', 'pipe_state'),
            'subscription',  jsonb_build_object('type', 'reference', 'value', $1),
            'emit_count',    jsonb_build_object('type', 'integer', 'value', 0),
            'window_start',  jsonb_build_object('type', 'timestamp', 'value', now()::text),
            'queue_depth',   jsonb_build_object('type', 'integer', 'value', 0),
            'dedup_hashes',  jsonb_build_object('type', 'json', 'value', '{}'),
            'last_emit',     jsonb_build_object('type', 'timestamp', 'value', ''),
            'last_ack',      jsonb_build_object('type', 'timestamp', 'value', ''),
            'failures',      jsonb_build_object('type', 'integer', 'value', 0),
            'status',        jsonb_build_object('type', 'utf8', 'value', 'open')
        ),
        '{SYSTEM}'
    ) RETURNING unid
""", ["text"])

row = plpy.execute(plan, [str(p_subscription_unid)])
return row[0]['unid']
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: gate — content filter. Returns true if blob may pass.
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.gate(
    p_blob_unid UUID,
    p_gate_expr TEXT
) RETURNS BOOLEAN AS $$
import json

if not p_gate_expr or p_gate_expr == '*':
    return True

row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_blob_unid])

if not row:
    return False

fields = row[0]['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

# Parse gate expressions: "composition!=secret", "size<10485760", "composition=file"
# Multiple conditions separated by comma: "composition!=secret,size<104857600"
conditions = [c.strip() for c in p_gate_expr.split(',')]

for cond in conditions:
    if '!=' in cond:
        key, val = cond.split('!=', 1)
        blob_val = str(fields.get(key, {}).get('value', ''))
        if blob_val == val:
            return False
    elif '<=' in cond:
        key, val = cond.split('<=', 1)
        blob_val = fields.get(key, {}).get('value', 0)
        if int(blob_val) > int(val):
            return False
    elif '>=' in cond:
        key, val = cond.split('>=', 1)
        blob_val = fields.get(key, {}).get('value', 0)
        if int(blob_val) < int(val):
            return False
    elif '<' in cond:
        key, val = cond.split('<', 1)
        blob_val = fields.get(key, {}).get('value', 0)
        if int(blob_val) >= int(val):
            return False
    elif '>' in cond:
        key, val = cond.split('>', 1)
        blob_val = fields.get(key, {}).get('value', 0)
        if int(blob_val) <= int(val):
            return False
    elif '=' in cond:
        key, val = cond.split('=', 1)
        blob_val = str(fields.get(key, {}).get('value', ''))
        if blob_val != val:
            return False

return True
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: throttle — rate limiter. Returns true if emission is allowed.
-- Updates rate counters on the pipe_state blob.
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.throttle(
    p_pipe_state_unid UUID,
    p_rate_limit TEXT
) RETURNS BOOLEAN AS $$
import json, re
from datetime import datetime, timedelta

if not p_rate_limit:
    return True

# Parse rate limit: "10/minute", "100/hour", "1000/day"
match = re.match(r'(\d+)/(second|minute|hour|day)', p_rate_limit)
if not match:
    return True

max_count = int(match.group(1))
window = match.group(2)
window_seconds = {'second': 1, 'minute': 60, 'hour': 3600, 'day': 86400}[window]

row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_pipe_state_unid])

if not row:
    return True

fields = row[0]['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

emit_count = int(fields.get('emit_count', {}).get('value', 0))
window_start_str = fields.get('window_start', {}).get('value', '')

now = datetime.utcnow()

# Check if window has elapsed
try:
    window_start = datetime.fromisoformat(window_start_str.replace('+00:00', '').replace('Z', ''))
    elapsed = (now - window_start).total_seconds()
except:
    elapsed = window_seconds + 1

if elapsed >= window_seconds:
    # Reset window
    plpy.execute(plpy.prepare("""
        UPDATE substrate.blob SET fields = fields
            || jsonb_build_object(
                'emit_count', jsonb_build_object('type', 'integer', 'value', 1),
                'window_start', jsonb_build_object('type', 'timestamp', 'value', $1)
            )
        WHERE unid = $2
    """, ["text", "uuid"]), [now.isoformat() + 'Z', p_pipe_state_unid])
    return True

if emit_count >= max_count:
    return False

# Increment counter
plpy.execute(plpy.prepare("""
    UPDATE substrate.blob SET fields = jsonb_set(
        fields, '{emit_count,value}', to_jsonb(($1)::int)
    ) WHERE unid = $2
""", ["int", "uuid"]), [emit_count + 1, p_pipe_state_unid])

return True
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: dedup — deduplication check. Returns true if NOT a duplicate.
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.dedup(
    p_pipe_state_unid UUID,
    p_content_hash TEXT,
    p_dedup_window TEXT
) RETURNS BOOLEAN AS $$
import json
from datetime import datetime, timedelta

if not p_dedup_window or not p_content_hash:
    return True

# Parse ISO 8601 duration (simple: PT1H, PT30M, P1D)
window_seconds = 3600  # default 1 hour
if 'H' in p_dedup_window:
    import re
    m = re.search(r'(\d+)H', p_dedup_window)
    if m:
        window_seconds = int(m.group(1)) * 3600
elif 'M' in p_dedup_window:
    import re
    m = re.search(r'(\d+)M', p_dedup_window)
    if m:
        window_seconds = int(m.group(1)) * 60
elif 'D' in p_dedup_window:
    import re
    m = re.search(r'(\d+)D', p_dedup_window)
    if m:
        window_seconds = int(m.group(1)) * 86400

row = plpy.execute(plpy.prepare(
    "SELECT fields->'dedup_hashes'->>'value' as hashes FROM substrate.blob WHERE unid = $1",
    ["uuid"]
), [p_pipe_state_unid])

now = datetime.utcnow()
cache = {}
if row and row[0]['hashes']:
    try:
        cache = json.loads(row[0]['hashes'])
    except:
        cache = {}

# Evict expired entries
cutoff = (now - timedelta(seconds=window_seconds)).isoformat()
cache = {h: ts for h, ts in cache.items() if ts > cutoff}

# Check for duplicate
if p_content_hash in cache:
    return False

# Record this hash
cache[p_content_hash] = now.isoformat()

plpy.execute(plpy.prepare("""
    UPDATE substrate.blob SET fields = jsonb_set(
        fields, '{dedup_hashes,value}', to_jsonb($1::text)
    ) WHERE unid = $2
""", ["text", "uuid"]), [json.dumps(cache), p_pipe_state_unid])

return True
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: enqueue — when throttled, queue the emission for later
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.enqueue(
    p_subscription_unid UUID,
    p_blob_unid UUID,
    p_priority INT DEFAULT 0
) RETURNS UUID AS $$
import json

plan = plpy.prepare("""
    INSERT INTO substrate.blob (fields, subscriber)
    VALUES (
        jsonb_build_object(
            'composition',    jsonb_build_object('type', 'utf8', 'value', 'emission'),
            'subscription',   jsonb_build_object('type', 'reference', 'value', $1),
            'blob',           jsonb_build_object('type', 'reference', 'value', $2),
            'priority',       jsonb_build_object('type', 'integer', 'value', $3),
            'state',          jsonb_build_object('type', 'utf8', 'value', 'queued'),
            'queued_at',      jsonb_build_object('type', 'timestamp', 'value', now()::text),
            'attempts',       jsonb_build_object('type', 'integer', 'value', 0)
        ),
        '{SYSTEM}'
    ) RETURNING unid
""", ["text", "text", "int"])

row = plpy.execute(plan, [str(p_subscription_unid), str(p_blob_unid), p_priority])

# Increment queue depth on pipe_state
plpy.execute(plpy.prepare("""
    UPDATE substrate.blob SET fields = jsonb_set(
        fields, '{queue_depth,value}',
        to_jsonb((COALESCE((fields->'queue_depth'->>'value')::int, 0) + 1))
    )
    WHERE fields->'composition'->>'value' = 'pipe_state'
    AND fields->'subscription'->>'value' = $1
""", ["text"]), [str(p_subscription_unid)])

return row[0]['unid']
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: drain — process queued emissions, respecting rate limits
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.drain(
    p_subscription_unid UUID,
    p_max_batch INT DEFAULT 10
) RETURNS INT AS $$
import json

# Get subscription details
sub_row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_subscription_unid])

if not sub_row:
    return 0

sub_fields = sub_row[0]['fields']
if isinstance(sub_fields, str):
    sub_fields = json.loads(sub_fields)

endpoint = sub_fields.get('endpoint', {}).get('value', '')
protocol = sub_fields.get('protocol', {}).get('value', 'http')
compress_algo = sub_fields.get('compress', {}).get('value', None)
rate_limit = sub_fields.get('rate_limit', {}).get('value', None)

# Get pipe state
ps_row = plpy.execute(plpy.prepare("""
    SELECT unid FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'pipe_state'
    AND fields->'subscription'->>'value' = $1
    LIMIT 1
""", ["text"]), [str(p_subscription_unid)])

pipe_state_unid = ps_row[0]['unid'] if ps_row else None

# Get queued emissions, ordered by priority DESC
queued = plpy.execute(plpy.prepare("""
    SELECT unid, fields->'blob'->>'value' as blob_unid,
           (fields->'priority'->>'value')::int as priority
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'emission'
    AND fields->'subscription'->>'value' = $1
    AND fields->'state'->>'value' = 'queued'
    ORDER BY (fields->'priority'->>'value')::int DESC,
             fields->'queued_at'->>'value' ASC
    LIMIT $2
""", ["text", "int"]), [str(p_subscription_unid), p_max_batch])

drained = 0
for emission in queued:
    # Check rate limit
    if pipe_state_unid and rate_limit:
        allowed = plpy.execute(plpy.prepare(
            "SELECT substrate.throttle($1, $2) as ok", ["uuid", "text"]
        ), [pipe_state_unid, rate_limit])
        if not allowed[0]['ok']:
            break

    blob_unid = emission['blob_unid']

    # Compress if needed
    source_unid = blob_unid
    if compress_algo:
        try:
            comp_row = plpy.execute(plpy.prepare(
                "SELECT substrate.compress($1::uuid, $2) as unid", ["text", "text"]
            ), [blob_unid, compress_algo])
            source_unid = comp_row[0]['unid']
        except:
            pass

    # Emit
    try:
        plpy.execute(plpy.prepare(
            "SELECT substrate.emit($1::uuid, $2, $3, false)", ["text", "text", "text"]
        ), [str(source_unid), endpoint, protocol])

        # Mark emission as delivered
        plpy.execute(plpy.prepare("""
            UPDATE substrate.blob SET fields = fields
                || jsonb_build_object(
                    'state', jsonb_build_object('type', 'utf8', 'value', 'delivered'),
                    'delivered_at', jsonb_build_object('type', 'timestamp', 'value', now()::text)
                )
            WHERE unid = $1
        """, ["uuid"]), [emission['unid']])
        drained += 1

    except Exception as e:
        # Mark emission as failed, increment attempts
        plpy.execute(plpy.prepare("""
            UPDATE substrate.blob SET fields = fields
                || jsonb_build_object(
                    'state', jsonb_build_object('type', 'utf8', 'value', 'failed'),
                    'last_error', jsonb_build_object('type', 'utf8', 'value', $1),
                    'attempts', jsonb_build_object('type', 'integer', 'value',
                        COALESCE((fields->'attempts'->>'value')::int, 0) + 1)
                )
            WHERE unid = $2
        """, ["text", "uuid"]), [str(e)[:500], emission['unid']])

        # Increment failure count on pipe state
        if pipe_state_unid:
            plpy.execute(plpy.prepare("""
                UPDATE substrate.blob SET fields = jsonb_set(
                    fields, '{failures,value}',
                    to_jsonb((COALESCE((fields->'failures'->>'value')::int, 0) + 1))
                ) WHERE unid = $1
            """, ["uuid"]), [pipe_state_unid])

# Update queue depth
if drained > 0 and pipe_state_unid:
    plpy.execute(plpy.prepare("""
        UPDATE substrate.blob SET fields = jsonb_set(
            fields, '{queue_depth,value}',
            to_jsonb(GREATEST(0, COALESCE((fields->'queue_depth'->>'value')::int, 0) - $1))
        ) WHERE unid = $2
    """, ["int", "uuid"]), [drained, pipe_state_unid])

return drained
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: circuit_breaker — if failures exceed threshold, trip the pipe
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.circuit_breaker(
    p_pipe_state_unid UUID,
    p_max_failures INT DEFAULT 5,
    p_cooldown_seconds INT DEFAULT 300
) RETURNS TEXT AS $$
import json
from datetime import datetime, timedelta

row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_pipe_state_unid])

if not row:
    return 'no_state'

fields = row[0]['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

failures = int(fields.get('failures', {}).get('value', 0))
status = fields.get('status', {}).get('value', 'open')

if status == 'tripped':
    # Check if cooldown has elapsed
    last_emit_str = fields.get('last_emit', {}).get('value', '')
    if last_emit_str:
        try:
            last_emit = datetime.fromisoformat(last_emit_str.replace('+00:00', '').replace('Z', ''))
            elapsed = (datetime.utcnow() - last_emit).total_seconds()
            if elapsed >= p_cooldown_seconds:
                # Reset to half-open
                plpy.execute(plpy.prepare("""
                    UPDATE substrate.blob SET fields = fields
                        || jsonb_build_object(
                            'status', jsonb_build_object('type', 'utf8', 'value', 'half_open'),
                            'failures', jsonb_build_object('type', 'integer', 'value', 0)
                        )
                    WHERE unid = $1
                """, ["uuid"]), [p_pipe_state_unid])
                return 'half_open'
        except:
            pass
    return 'tripped'

if failures >= p_max_failures:
    # Trip the breaker
    plpy.execute(plpy.prepare("""
        UPDATE substrate.blob SET fields = fields
            || jsonb_build_object(
                'status', jsonb_build_object('type', 'utf8', 'value', 'tripped'),
                'tripped_at', jsonb_build_object('type', 'timestamp', 'value', $1)
            )
        WHERE unid = $2
    """, ["text", "uuid"]), [datetime.utcnow().isoformat() + 'Z', p_pipe_state_unid])

    plpy.notice(f'Circuit breaker TRIPPED after {failures} failures')
    return 'tripped'

return status
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: ack — receiver acknowledges delivery
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.ack(p_emission_unid UUID)
RETURNS VOID AS $$
from datetime import datetime

plpy.execute(plpy.prepare("""
    UPDATE substrate.blob SET fields = fields
        || jsonb_build_object(
            'state', jsonb_build_object('type', 'utf8', 'value', 'acknowledged'),
            'acked_at', jsonb_build_object('type', 'timestamp', 'value', $1)
        )
    WHERE unid = $2
    AND fields->'composition'->>'value' = 'emission'
""", ["text", "uuid"]), [datetime.utcnow().isoformat() + 'Z', p_emission_unid])

sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'ack', '{}', '00000000-0000-0000-0000-000000000001')
""", ["uuid"])
plpy.execute(sig, [p_emission_unid])
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: governed_propagate — the full governed pipeline
-- Replaces raw propagate() with the complete force chain:
-- gate → dedup → throttle → (compress → emit) OR enqueue
-- With circuit breaker protection.
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.governed_propagate(p_blob_unid UUID)
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

# Find all active subscriptions
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
    'gated': 0,
    'deduped': 0,
    'throttled': 0,
    'emitted': 0,
    'queued': 0,
    'circuit_broken': 0,
    'errors': 0
}

for sub in subs:
    sub_fields = sub['fields']
    if isinstance(sub_fields, str):
        sub_fields = json.loads(sub_fields)

    target = sub_fields.get('target', {}).get('value', '')
    sub_unid = sub['unid']

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

    # === GOVERNOR CHAIN ===

    # 1. GATE — content filter
    gate_expr = sub_fields.get('gate', {}).get('value', '')
    if gate_expr:
        gated = plpy.execute(plpy.prepare(
            "SELECT substrate.gate($1, $2) as ok", ["uuid", "text"]
        ), [p_blob_unid, gate_expr])
        if not gated[0]['ok']:
            results['gated'] += 1
            continue

    # 2. Ensure pipe state exists
    ps_row = plpy.execute(plpy.prepare(
        "SELECT substrate.ensure_pipe_state($1) as unid", ["uuid"]
    ), [sub_unid])
    pipe_state_unid = ps_row[0]['unid']

    # 3. CIRCUIT BREAKER — is the pipe tripped?
    cb_status = plpy.execute(plpy.prepare(
        "SELECT substrate.circuit_breaker($1) as status", ["uuid"]
    ), [pipe_state_unid])
    if cb_status[0]['status'] == 'tripped':
        results['circuit_broken'] += 1
        continue

    # 4. DEDUP — did we already send this hash?
    dedup_window = sub_fields.get('dedup', {}).get('value', '')
    if dedup_window:
        is_new = plpy.execute(plpy.prepare(
            "SELECT substrate.dedup($1, $2, $3) as ok", ["uuid", "text", "text"]
        ), [pipe_state_unid, content_hash, dedup_window])
        if not is_new[0]['ok']:
            results['deduped'] += 1
            continue

    # 5. THROTTLE — are we within rate limits?
    rate_limit = sub_fields.get('rate_limit', {}).get('value', '')
    allowed = True
    if rate_limit:
        throttle_result = plpy.execute(plpy.prepare(
            "SELECT substrate.throttle($1, $2) as ok", ["uuid", "text"]
        ), [pipe_state_unid, rate_limit])
        allowed = throttle_result[0]['ok']

    if not allowed:
        # Check backpressure policy
        bp_policy = sub_fields.get('backpressure', {}).get('value', 'queue')
        if bp_policy == 'drop':
            results['throttled'] += 1
            continue
        elif bp_policy == 'queue':
            priority = int(sub_fields.get('priority', {}).get('value', 0))
            plpy.execute(plpy.prepare(
                "SELECT substrate.enqueue($1, $2, $3)", ["uuid", "uuid", "int"]
            ), [sub_unid, p_blob_unid, priority])
            results['queued'] += 1
            continue
        else:
            # 'slow' — queue it but also try drain
            priority = int(sub_fields.get('priority', {}).get('value', 0))
            plpy.execute(plpy.prepare(
                "SELECT substrate.enqueue($1, $2, $3)", ["uuid", "uuid", "int"]
            ), [sub_unid, p_blob_unid, priority])
            results['queued'] += 1
            continue

    # 6. COMPRESS + EMIT — the blob traverses the pipe
    endpoint = sub_fields.get('endpoint', {}).get('value', '')
    protocol = sub_fields.get('protocol', {}).get('value', 'http')
    compress_algo = sub_fields.get('compress', {}).get('value', None)

    source_unid = p_blob_unid
    if compress_algo and blob_fields.get('content', {}).get('type') != 'compressed':
        try:
            comp_row = plpy.execute(plpy.prepare(
                "SELECT substrate.compress($1, $2) as unid", ["uuid", "text"]
            ), [p_blob_unid, compress_algo])
            source_unid = comp_row[0]['unid']
        except:
            pass

    try:
        plpy.execute(plpy.prepare(
            "SELECT substrate.emit($1, $2, $3, false)", ["uuid", "text", "text"]
        ), [source_unid, endpoint, protocol])

        # Update pipe state
        plpy.execute(plpy.prepare("""
            UPDATE substrate.blob SET fields = fields
                || jsonb_build_object(
                    'last_emit', jsonb_build_object('type', 'timestamp', 'value', now()::text),
                    'failures', jsonb_build_object('type', 'integer', 'value', 0)
                )
            WHERE unid = $1
        """, ["uuid"]), [pipe_state_unid])

        # If half_open succeeded, reopen
        if cb_status[0]['status'] == 'half_open':
            plpy.execute(plpy.prepare("""
                UPDATE substrate.blob SET fields = jsonb_set(
                    fields, '{status,value}', '"open"'::jsonb
                ) WHERE unid = $1
            """, ["uuid"]), [pipe_state_unid])

        results['emitted'] += 1

    except Exception as e:
        # Increment failures on pipe state
        plpy.execute(plpy.prepare("""
            UPDATE substrate.blob SET fields = jsonb_set(
                fields, '{failures,value}',
                to_jsonb((COALESCE((fields->'failures'->>'value')::int, 0) + 1))
            ) WHERE unid = $1
        """, ["uuid"]), [pipe_state_unid])
        results['errors'] += 1

# Signal
if results['emitted'] > 0 or results['queued'] > 0:
    sig = plpy.prepare("""
        INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
        VALUES ($1, 'governed_propagate', $2::jsonb,
                '00000000-0000-0000-0000-000000000001')
    """, ["uuid", "text"])
    plpy.execute(sig, [p_blob_unid, json.dumps(results)])

return json.dumps(results)
$$ LANGUAGE plpython3u;
