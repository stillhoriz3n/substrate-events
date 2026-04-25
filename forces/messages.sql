CREATE OR REPLACE FUNCTION substrate.messages(p_status text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE sql
AS $function$ SELECT substrate.inbox(current_setting('substrate.identity', true), p_status) $function$
