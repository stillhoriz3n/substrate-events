CREATE OR REPLACE FUNCTION substrate.announce(p_what text, p_to text[] DEFAULT ARRAY['oa:matt'::text, 'oa:joey'::text, 'oa:vision'::text, 'oa:jarvis'::text, 'oa:kevin'::text, 'oa:ari'::text])
 RETURNS uuid[]
 LANGUAGE sql
AS $function$ SELECT substrate.broadcast(current_setting('substrate.identity', true), p_to, p_what, 'announcement') $function$
