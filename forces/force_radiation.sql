CREATE OR REPLACE FUNCTION substrate.force_radiation()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM pg_notify('blob_changed', json_build_object(
        'unid', NEW.unid,
        'composition', NEW.fields->'composition'->>'value',
        'ordinal', NEW.ordinal,
        'hash', encode(NEW.content_hash, 'hex')
    )::text);
    RETURN NEW;
END;
$function$
