CREATE OR REPLACE FUNCTION substrate.zip(p_blob_unid uuid)
 RETURNS uuid
 LANGUAGE sql
AS $function$ SELECT substrate.compress(p_blob_unid, 'zlib') $function$
