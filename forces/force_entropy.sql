CREATE OR REPLACE FUNCTION substrate.force_entropy()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    -- Check if the reaper is active (it's the only force allowed to delete)
    IF current_setting('substrate.reaper_active', true) = 'true' THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'blobs cannot be deleted, only retired';
END;
$function$
