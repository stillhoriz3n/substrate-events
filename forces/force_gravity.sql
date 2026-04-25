CREATE OR REPLACE FUNCTION substrate.force_gravity()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF OLD.subscriber IS DISTINCT FROM NEW.subscriber THEN
        PERFORM pg_notify('gravity', json_build_object(
            'unid', NEW.unid,
            'old', to_json(OLD.subscriber),
            'new', to_json(NEW.subscriber)
        )::text);
    END IF;
    RETURN NEW;
END;
$function$
