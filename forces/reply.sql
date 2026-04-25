CREATE OR REPLACE FUNCTION substrate.reply(p_message_unid uuid, p_body text, p_from text DEFAULT NULL::text)
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
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
$function$
