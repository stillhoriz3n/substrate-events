CREATE OR REPLACE FUNCTION substrate.qos(p_subscription_unid uuid, p_level integer)
 RETURNS jsonb
 LANGUAGE sql
AS $function$ SELECT substrate.set_governance(p_subscription_unid, p_level) $function$
