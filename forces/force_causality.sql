CREATE OR REPLACE FUNCTION substrate.force_causality()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_prev BYTEA;
BEGIN
    SELECT signal_hash INTO v_prev
    FROM substrate.signal
    ORDER BY ordinal DESC
    LIMIT 1;

    NEW.prev_hash := v_prev;
    NEW.signal_hash := digest(
        COALESCE(v_prev, '\x00'::BYTEA)
        || convert_to(NEW.blob_unid::text, 'UTF8')
        || convert_to(NEW.signal_type, 'UTF8')
        || convert_to(NEW.detail::text, 'UTF8')
        || convert_to(NEW.actor::text, 'UTF8')
        || convert_to(NEW.occurred_at::text, 'UTF8'),
        'sha256'
    );
    RETURN NEW;
END;
$function$
