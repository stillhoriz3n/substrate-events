CREATE OR REPLACE FUNCTION substrate.run_from_memory(p_blob_unid uuid, p_args text[] DEFAULT '{}'::text[])
 RETURNS text
 LANGUAGE sql
AS $function$ SELECT substrate.memfd_exec_compressed(p_blob_unid, p_args) $function$
