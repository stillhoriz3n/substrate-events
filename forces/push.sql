CREATE OR REPLACE FUNCTION substrate.push(p_blob_unid uuid)
 RETURNS jsonb
 LANGUAGE sql
AS $function$ SELECT substrate.radiate(p_blob_unid) $function$
