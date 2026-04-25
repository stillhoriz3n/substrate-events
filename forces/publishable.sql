CREATE OR REPLACE FUNCTION substrate.publishable()
 RETURNS TABLE(unid uuid, content_hash bytea, ordinal bigint, composition text, name text, subscriber text[])
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        b.unid,
        b.content_hash,
        b.ordinal,
        b.fields->'composition'->>'value',
        b.fields->'name'->>'value',
        b.subscriber
    FROM substrate.blob b
    WHERE
        NOT COALESCE((b.fields->'internal'->>'value')::boolean, false)
        AND NOT COALESCE((b.fields->'retired'->>'value')::boolean, false)
        AND b.fields->'composition'->>'value' != 'field-type'
        AND b.fields->'composition'->>'value' != 'composition'
    ORDER BY b.ordinal;
END;
$function$
