CREATE OR REPLACE FUNCTION substrate.unzip(p_blob_unid uuid)
 RETURNS bytea
 LANGUAGE sql
AS $function$ SELECT substrate.decompress(p_blob_unid) $function$
