CREATE OR REPLACE FUNCTION substrate.mtu_check(p_blob_unid uuid, p_max_bytes integer)
 RETURNS boolean
 LANGUAGE sql
AS $function$ SELECT substrate.gate(p_blob_unid, 'size<' || p_max_bytes::text) $function$
