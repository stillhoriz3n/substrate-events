CREATE OR REPLACE FUNCTION substrate.kill(p_unid uuid)
 RETURNS void
 LANGUAGE sql
AS $function$ SELECT substrate.retire(p_unid) $function$
