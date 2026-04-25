CREATE OR REPLACE FUNCTION substrate.broadcast(p_from text, p_to text[], p_body text, p_subject text DEFAULT ''::text)
 RETURNS uuid[]
 LANGUAGE plpython3u
AS $function$
import json

msg_unids = []
for recipient in p_to:
    row = plpy.execute(plpy.prepare(
        "SELECT substrate.send($1, $2, $3, $4, 'broadcast') as unid",
        ["text", "text", "text", "text"]
    ), [p_from, recipient, p_body, p_subject])
    msg_unids.append(str(row[0]['unid']))

return msg_unids
$function$
