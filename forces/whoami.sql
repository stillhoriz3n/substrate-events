CREATE OR REPLACE FUNCTION substrate.whoami()
 RETURNS TABLE(unid uuid, name text, composition text)
 LANGUAGE sql
AS $function$
    SELECT unid,
           fields->'name'->>'value',
           fields->'composition'->>'value'
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'principal'
    LIMIT 1
$function$
