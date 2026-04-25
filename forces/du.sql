CREATE OR REPLACE FUNCTION substrate.du()
 RETURNS jsonb
 LANGUAGE sql
AS $function$ SELECT substrate.vacuum_report() $function$
