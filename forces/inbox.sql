CREATE OR REPLACE FUNCTION substrate.inbox(p_address text, p_status text DEFAULT NULL::text, p_limit integer DEFAULT 50)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
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
$function$
