CREATE OR REPLACE FUNCTION substrate.pipe_open(p_name text, p_target text, p_endpoint text, p_qos integer DEFAULT 50)
 RETURNS uuid
 LANGUAGE sql
AS $function$ SELECT substrate.route_add(p_name, p_target, p_endpoint, 'pg', p_qos) $function$
