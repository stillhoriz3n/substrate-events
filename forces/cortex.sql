-- ============================================================
-- CORTEX PRIMITIVES — messaging is blobs on pipes
--
-- A message is a blob. The subscriber field is the address.
-- send() creates the blob. governed_propagate() delivers it.
-- inbox() queries your messages. ack_message() marks completion.
--
-- The same force that moves a binary moves a message.
-- The same governor that rate-limits a pipe rate-limits a conversation.
-- The same gate that blocks secrets can block spam.
-- ============================================================

-- Field types for messaging
INSERT INTO substrate.blob (unid, fields, subscriber) VALUES
('00000000-0000-0000-0001-000000000080',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"address"},"description":{"type":"utf8","value":"Cortex address — oa:matt, oa:joey, oa:vision. The identity of a principal on the mesh."}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000081',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"message_type"},"description":{"type":"utf8","value":"Kind of message — dm, broadcast, command, reply, ack"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000082',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"subject"},"description":{"type":"utf8","value":"Message subject line"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000083',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"body"},"description":{"type":"utf8","value":"Message body — the content of the message"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000084',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"thread"},"description":{"type":"utf8","value":"Reference to the parent message — threading for conversations"}}',
 '{SYSTEM}'),
('00000000-0000-0000-0001-000000000085',
 '{"composition":{"type":"utf8","value":"field_type"},"name":{"type":"utf8","value":"status"},"description":{"type":"utf8","value":"Message lifecycle — pending, delivered, read, completed"}}',
 '{SYSTEM}');

-- Composition: message
INSERT INTO substrate.blob (unid, fields, subscriber) VALUES
('00000000-0000-0000-0002-000000000040',
 '{"composition":{"type":"utf8","value":"composition"},"name":{"type":"utf8","value":"message"},"description":{"type":"utf8","value":"A cortex message between principals. The blob IS the message. The subscriber IS the recipient. Propagation IS delivery."}}',
 '{SYSTEM}');

-- ============================================================
-- FORCE: send — create a message blob, propagate to recipient
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.send(
    p_from TEXT,
    p_to TEXT,
    p_body TEXT,
    p_subject TEXT DEFAULT '',
    p_message_type TEXT DEFAULT 'dm',
    p_thread UUID DEFAULT NULL,
    p_priority INT DEFAULT 0
) RETURNS UUID AS $$
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
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: inbox — query messages addressed to you
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.inbox(
    p_address TEXT,
    p_status TEXT DEFAULT NULL,
    p_limit INT DEFAULT 50
) RETURNS JSONB AS $$
import json

principal = p_address.replace('oa:', '') if p_address.startswith('oa:') else p_address

query = """
    SELECT unid,
           fields->'from'->>'value' as sender,
           fields->'to'->>'value' as recipient,
           fields->'subject'->>'value' as subject,
           fields->'body'->>'value' as body,
           fields->'message_type'->>'value' as message_type,
           fields->'status'->>'value' as status,
           fields->'sent_at'->>'value' as sent_at,
           fields->'thread'->>'value' as thread,
           fields->'priority'->>'value' as priority
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'message'
    AND $1 = ANY(subscriber)
    AND fields->'to'->>'value' LIKE '%' || $1 || '%'
"""

params = ["text"]
args = [principal]

if p_status:
    query += " AND fields->'status'->>'value' = $2"
    params.append("text")
    args.append(p_status)

query += " ORDER BY fields->'sent_at'->>'value' DESC LIMIT $" + str(len(params) + 1)
params.append("int")
args.append(p_limit)

rows = plpy.execute(plpy.prepare(query, params), args)

messages = []
for row in rows:
    messages.append({
        'unid': str(row['unid']),
        'from': row['sender'],
        'to': row['recipient'],
        'subject': row['subject'],
        'body': row['body'][:500] if row['body'] else '',
        'message_type': row['message_type'],
        'status': row['status'],
        'sent_at': row['sent_at'],
        'thread': row['thread'],
        'priority': row['priority']
    })

return json.dumps({'address': p_address, 'count': len(messages), 'messages': messages})
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: ack_message — mark a message as delivered/read/completed
-- Forward-only: pending → delivered → read → completed
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.ack_message(
    p_message_unid UUID,
    p_status TEXT DEFAULT 'completed',
    p_result TEXT DEFAULT NULL
) RETURNS VOID AS $$
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
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: reply — reply to a message, threading the conversation
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.reply(
    p_message_unid UUID,
    p_body TEXT,
    p_from TEXT DEFAULT NULL
) RETURNS UUID AS $$
import json

row = plpy.execute(plpy.prepare("""
    SELECT fields->'from'->>'value' as sender,
           fields->'to'->>'value' as recipient,
           fields->'subject'->>'value' as subject,
           fields->'thread'->>'value' as thread
    FROM substrate.blob WHERE unid = $1
""", ["uuid"]), [p_message_unid])

if not row:
    plpy.error(f'message {p_message_unid} not found')

original = row[0]
reply_from = p_from or original['recipient']
reply_to = original['sender']
subject = 'Re: ' + (original['subject'] or '')
thread = original['thread'] or str(p_message_unid)

plan = plpy.prepare("""
    SELECT substrate.send($1, $2, $3, $4, 'reply', $5::uuid)
""", ["text", "text", "text", "text", "uuid"])

result = plpy.execute(plan, [reply_from, reply_to, p_body, subject, thread])
return result[0]['send']
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: broadcast — send a message to multiple recipients
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.broadcast(
    p_from TEXT,
    p_to TEXT[],
    p_body TEXT,
    p_subject TEXT DEFAULT ''
) RETURNS UUID[] AS $$
import json

msg_unids = []
for recipient in p_to:
    row = plpy.execute(plpy.prepare(
        "SELECT substrate.send($1, $2, $3, $4, 'broadcast') as unid",
        ["text", "text", "text", "text"]
    ), [p_from, recipient, p_body, p_subject])
    msg_unids.append(str(row[0]['unid']))

return msg_unids
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: thread — get all messages in a conversation thread
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.thread(p_thread_unid UUID)
RETURNS JSONB AS $$
import json

rows = plpy.execute(plpy.prepare("""
    SELECT unid,
           fields->'from'->>'value' as sender,
           fields->'to'->>'value' as recipient,
           fields->'body'->>'value' as body,
           fields->'status'->>'value' as status,
           fields->'sent_at'->>'value' as sent_at
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'message'
    AND (fields->'thread'->>'value' = $1 OR unid = $2)
    ORDER BY fields->'sent_at'->>'value' ASC
""", ["text", "uuid"]), [str(p_thread_unid), p_thread_unid])

messages = []
for row in rows:
    messages.append({
        'unid': str(row['unid']),
        'from': row['sender'],
        'to': row['recipient'],
        'body': row['body'][:500] if row['body'] else '',
        'status': row['status'],
        'sent_at': row['sent_at']
    })

return json.dumps({'thread': str(p_thread_unid), 'count': len(messages), 'messages': messages})
$$ LANGUAGE plpython3u;
