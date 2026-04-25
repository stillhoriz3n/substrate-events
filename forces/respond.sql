CREATE OR REPLACE FUNCTION substrate.respond(p_message_unid uuid, p_body text)
 RETURNS uuid
 LANGUAGE sql
AS $function$ SELECT substrate.reply(p_message_unid, p_body) $function$
