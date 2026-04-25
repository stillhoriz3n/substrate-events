CREATE OR REPLACE FUNCTION substrate.set_governance(p_subscription_unid uuid, p_level integer)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
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
$function$
