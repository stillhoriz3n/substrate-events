CREATE OR REPLACE FUNCTION substrate.pull()
 RETURNS text
 LANGUAGE sql
AS $function$ SELECT 'pull: load forces/poll.sql first'::text $function$
