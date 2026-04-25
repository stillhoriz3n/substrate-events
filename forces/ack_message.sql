CREATE OR REPLACE FUNCTION substrate.ack_message(p_message_unid uuid, p_status text DEFAULT 'completed'::text, p_result text DEFAULT NULL::text)
 RETURNS void
 LANGUAGE plpython3u
AS $function$
import json

valid_transitions = {
    'pending': ['delivered', 'read', 'completed'],
    'delivered': ['read', 'completed'],
    'read': ['completed'],
}

row = plpy.execute(plpy.prepare(
    "SELECT fields->'status'->>'value' as status FROM substrate.blob WHERE unid = $1",
    ["uuid"]
), [p_message_unid])

if not row:
    plpy.error(f'message {p_message_unid} not found')

current = row[0]['status']
if p_status not in valid_transitions.get(current, []):
    plpy.error(f'invalid transition: {current} → {p_status}')

updates = {
    'status': {'type': 'status', 'value': p_status},
}

if p_status == 'delivered':
    updates['delivered_at'] = {'type': 'timestamp', 'value': ''}
elif p_status == 'read':
    updates['read_at'] = {'type': 'timestamp', 'value': ''}
elif p_status == 'completed':
    updates['completed_at'] = {'type': 'timestamp', 'value': ''}

if p_result:
    updates['result'] = {'type': 'utf8', 'value': p_result}

plan = plpy.prepare("""
    UPDATE substrate.blob SET fields = fields || $1::jsonb
    WHERE unid = $2
""", ["text", "uuid"])
plpy.execute(plan, [json.dumps(updates), p_message_unid])

# Set the timestamp
ts_field = {'delivered': 'delivered_at', 'read': 'read_at', 'completed': 'completed_at'}.get(p_status)
if ts_field:
    plpy.execute(plpy.prepare(f"""
        UPDATE substrate.blob SET fields = jsonb_set(
            fields, '{{{ts_field},value}}', to_jsonb(now()::text)
        ) WHERE unid = $1
    """, ["uuid"]), [p_message_unid])

sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'ack_message', jsonb_build_object('status', $2),
            '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text"])
plpy.execute(sig, [p_message_unid, p_status])
$function$
