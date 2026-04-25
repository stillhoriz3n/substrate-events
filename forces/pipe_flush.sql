CREATE OR REPLACE FUNCTION substrate.pipe_flush(p_subscription_unid uuid)
 RETURNS integer
 LANGUAGE sql
AS $function$ SELECT substrate.drain(p_subscription_unid) $function$
