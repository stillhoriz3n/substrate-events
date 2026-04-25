CREATE OR REPLACE FUNCTION substrate.ps()
 RETURNS TABLE(unid uuid, name text, exitcode text, state text)
 LANGUAGE sql
AS $function$
    SELECT unid,
           fields->'name'->>'value',
           fields->'exitcode'->>'value',
           fields->'state'->>'value'
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'process'
$function$
