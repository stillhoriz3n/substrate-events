CREATE OR REPLACE FUNCTION substrate.ship(p_blob_unid uuid, p_endpoint text, p_protocol text DEFAULT 'http'::text)
 RETURNS text
 LANGUAGE sql
AS $function$ SELECT substrate.emit(p_blob_unid, p_endpoint, p_protocol, true) $function$
