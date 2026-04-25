-- ============================================================
-- MESSAGES TRAVEL AT THE SPEED OF LIGHT
--
-- governed_propagate_v2 updated: if composition=message,
-- skip the entire governor chain. No gate, no throttle,
-- no dedup, no circuit breaker, no queue. Straight to emit.
--
-- Messages are the nervous system, not cargo.
-- Data gets governed. Thoughts travel free.
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

# MESSAGES TRAVEL AT THE SPEED OF LIGHT
is_message = blob_composition == 'message'

subs = plpy.execute("""
    SELECT unid, fields FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'subscription'
    AND (fields->'state' IS NULL OR fields->'state'->>'value' != 'retired')
""")

results = {
    'blob': str(p_blob_unid),
    'composition': blob_composition,
    'name': blob_name,
    'lightspeed': is_message,
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

    endpoint = sub_fields.get('endpoint', {}).get('value', '')
    protocol = sub_fields.get('protocol', {}).get('value', 'http')

    # === LIGHTSPEED PATH — messages skip all governors ===
    if is_message:
        try:
            plpy.execute(plpy.prepare(
                "SELECT substrate.emit($1, $2, $3, false)", ["uuid", "text", "text"]
            ), [p_blob_unid, endpoint, protocol])
            results['emitted'] += 1
        except Exception as e:
            results['errors'] += 1
        continue

    # === GOVERNED PATH — everything else goes through the chain ===
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
