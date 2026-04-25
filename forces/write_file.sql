CREATE OR REPLACE FUNCTION substrate.write_file(p_blob_unid uuid)
 RETURNS text
 LANGUAGE sql
AS $function$ SELECT substrate.materialize(p_blob_unid) $function$
