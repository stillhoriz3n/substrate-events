CREATE OR REPLACE FUNCTION substrate.done(p_message_unid uuid)
 RETURNS void
 LANGUAGE sql
AS $function$ SELECT substrate.ack_message(p_message_unid, 'completed') $function$
