CREATE OR REPLACE FUNCTION substrate.send(p_from text, p_to text, p_body text, p_subject text DEFAULT ''::text, p_message_type text DEFAULT 'dm'::text, p_thread uuid DEFAULT NULL::uuid, p_priority integer DEFAULT 0)
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
import json

fields = {
    'composition':  {'type': 'utf8', 'value': 'message'},
    'from':         {'type': 'address', 'value': p_from},
    'to':           {'type': 'address', 'value': p_to},
    'body':         {'type': 'body', 'value': p_body},
    'subject':      {'type': 'subject', 'value': p_subject},
    'message_type': {'type': 'message_type', 'value': p_message_type},
    'status':       {'type': 'status', 'value': 'pending'},
    'priority':     {'type': 'integer', 'value': p_priority},
    'sent_at':      {'type': 'timestamp', 'value': ''}
}

if p_thread:
    fields['thread'] = {'type': 'thread', 'value': str(p_thread)}

# Subscriber = [sender, recipient] — both can see it
# The recipient's name is extracted from the address (oa:joey → joey)
recipient_name = p_to.replace('oa:', '') if p_to.startswith('oa:') else p_to
sender_name = p_from.replace('oa:', '') if p_from.startswith('oa:') else p_from

plan = plpy.prepare("""
    INSERT INTO substrate.blob (fields, subscriber)
    VALUES ($1::jsonb, ARRAY[$2, $3])
    RETURNING unid
""", ["text", "text", "text"])

row = plpy.execute(plan, [json.dumps(fields), sender_name, recipient_name])
msg_unid = row[0]['unid']

# Set sent_at
plpy.execute(plpy.prepare("""
    UPDATE substrate.blob SET fields = jsonb_set(
        fields, '{sent_at,value}', to_jsonb(now()::text)
    ) WHERE unid = $1
""", ["uuid"]), [msg_unid])

# Signal
sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'send', jsonb_build_object(
        'from', $2, 'to', $3, 'subject', $4, 'type', $5
    ), '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text", "text", "text", "text"])
plpy.execute(sig, [msg_unid, p_from, p_to, p_subject, p_message_type])

# Propagate — the message blob flows through any matching subscriptions
# If the recipient has a subscription for composition=message, it arrives
try:
    plpy.execute(plpy.prepare(
        "SELECT substrate.governed_propagate_v2($1)", ["uuid"]
    ), [msg_unid])
except:
    pass

return msg_unid
$function$
