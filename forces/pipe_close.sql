CREATE OR REPLACE FUNCTION substrate.pipe_close(p_subscription_unid uuid)
 RETURNS void
 LANGUAGE sql
AS $function$ SELECT substrate.unsubscribe(p_subscription_unid) $function$
