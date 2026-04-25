CREATE OR REPLACE FUNCTION substrate.thread(p_thread_unid uuid)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
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
$function$
