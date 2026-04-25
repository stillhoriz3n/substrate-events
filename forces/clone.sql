CREATE OR REPLACE FUNCTION substrate.clone(p_peer text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE sql
AS $function$ SELECT 'clone: load forces/poll.sql first'::text $function$
