CREATE OR REPLACE FUNCTION substrate.ls(p_composition text DEFAULT 'file'::text)
 RETURNS TABLE(unid uuid, name text, size text, content_type text)
 LANGUAGE sql
AS $function$
    SELECT unid,
           fields->'name'->>'value',
           COALESCE(fields->'original_size'->>'value', fields->'size'->>'value', '0'),
           COALESCE(fields->'content'->>'type', '-')
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = p_composition
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
$function$
