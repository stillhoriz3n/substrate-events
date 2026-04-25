CREATE OR REPLACE FUNCTION substrate.census()
 RETURNS TABLE(metric text, value bigint)
 LANGUAGE sql
AS $function$
    SELECT 'blobs', count(*) FROM substrate.blob
    UNION ALL SELECT 'signals', count(*) FROM substrate.signal
    UNION ALL SELECT 'files', count(*) FROM substrate.blob WHERE fields->'composition'->>'value' = 'file'
    UNION ALL SELECT 'messages', count(*) FROM substrate.blob WHERE fields->'composition'->>'value' = 'message'
    UNION ALL SELECT 'subscriptions', count(*) FROM substrate.blob WHERE fields->'composition'->>'value' = 'subscription'
    UNION ALL SELECT 'retired', count(*) FROM substrate.blob WHERE retired_at IS NOT NULL
$function$
