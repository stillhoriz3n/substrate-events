CREATE OR REPLACE FUNCTION substrate.packet_broadcast(p_from text, p_to text[], p_payload text, p_subject text DEFAULT ''::text)
 RETURNS uuid[]
 LANGUAGE sql
AS $function$ SELECT substrate.broadcast(p_from, p_to, p_payload, p_subject) $function$
