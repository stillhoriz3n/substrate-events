CREATE OR REPLACE FUNCTION substrate.say(p_to text, p_what text)
 RETURNS uuid
 LANGUAGE sql
AS $function$ SELECT substrate.send(current_setting('substrate.identity', true), p_to, p_what, '', 'dm') $function$
