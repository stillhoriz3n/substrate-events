CREATE OR REPLACE FUNCTION substrate.download(p_blob_unid uuid, p_path text)
 RETURNS text
 LANGUAGE sql
AS $function$ SELECT substrate.emit(p_blob_unid, p_path, 'file', false) $function$
