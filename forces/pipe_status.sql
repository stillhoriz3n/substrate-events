CREATE OR REPLACE FUNCTION substrate.pipe_status()
 RETURNS TABLE(subscriber text, target text, endpoint text, governance integer, pipe_status text, queue_depth integer)
 LANGUAGE sql
AS $function$
    SELECT
        s.fields->'subscriber'->>'value',
        s.fields->'target'->>'value',
        s.fields->'endpoint'->>'value',
        COALESCE((s.fields->'governance'->>'value')::int, 50),
        COALESCE(p.fields->'status'->>'value', 'unknown'),
        COALESCE((p.fields->'queue_depth'->>'value')::int, 0)
    FROM substrate.blob s
    LEFT JOIN substrate.blob p ON p.fields->'composition'->>'value' = 'pipe_state'
        AND p.fields->'subscription'->>'value' = s.unid::text
    WHERE s.fields->'composition'->>'value' = 'subscription'
    AND (s.fields->'state' IS NULL OR s.fields->'state'->>'value' != 'retired')
$function$
