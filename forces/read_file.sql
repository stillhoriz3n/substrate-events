CREATE OR REPLACE FUNCTION substrate.read_file(p_path text)
 RETURNS uuid
 LANGUAGE sql
AS $function$ SELECT substrate.ingest(p_path) $function$
