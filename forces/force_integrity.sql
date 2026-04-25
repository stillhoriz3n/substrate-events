CREATE OR REPLACE FUNCTION substrate.force_integrity()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.content_hash := digest(convert_to(NEW.fields::text, 'UTF8'), 'sha256');
    NEW.ordinal := nextval('substrate.ordinal_seq');
    RETURN NEW;
END;
$function$
