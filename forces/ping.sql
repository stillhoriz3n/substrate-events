CREATE OR REPLACE FUNCTION substrate.ping(p_to text)
 RETURNS uuid
 LANGUAGE sql
AS $function$ SELECT substrate.send('SYSTEM', p_to, 'ping', 'ping', 'command') $function$
