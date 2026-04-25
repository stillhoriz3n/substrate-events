CREATE OR REPLACE FUNCTION substrate.route_add(p_subscriber text, p_filter text, p_endpoint text, p_protocol text DEFAULT 'pg'::text, p_qos integer DEFAULT 50)
 RETURNS uuid
 LANGUAGE sql
AS $function$
    SELECT sub_unid FROM (
        SELECT substrate.subscribe(p_subscriber, p_filter, p_endpoint, p_protocol) as sub_unid
    ) s, LATERAL substrate.set_governance(s.sub_unid, p_qos) g
$function$
