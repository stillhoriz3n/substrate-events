CREATE OR REPLACE FUNCTION substrate.firewall(p_blob_unid uuid, p_rule text)
 RETURNS boolean
 LANGUAGE sql
AS $function$ SELECT substrate.gate(p_blob_unid, p_rule) $function$
