CREATE OR REPLACE FUNCTION substrate.packet_capture(p_address text, p_limit integer DEFAULT 50)
 RETURNS jsonb
 LANGUAGE sql
AS $function$ SELECT substrate.inbox(p_address, NULL, p_limit) $function$
