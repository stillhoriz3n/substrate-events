CREATE OR REPLACE FUNCTION substrate.upload(p_path text)
 RETURNS uuid
 LANGUAGE sql
AS $function$ SELECT substrate.ingest_compressed(p_path, 'zlib') $function$
