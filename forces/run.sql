CREATE OR REPLACE FUNCTION substrate.run(p_file_unid uuid, p_args text[] DEFAULT '{}'::text[])
 RETURNS uuid
 LANGUAGE sql
AS $function$ SELECT substrate.exec(p_file_unid, p_args) $function$
