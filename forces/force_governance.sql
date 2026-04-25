CREATE OR REPLACE FUNCTION substrate.force_governance()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_composition TEXT;
    v_name TEXT;
    v_field_key TEXT;
    v_field_val JSONB;
    v_existing UUID;
    v_known_comp BOOLEAN;
BEGIN
    -- Extract composition and name
    v_composition := NEW.fields->'composition'->>'value';
    v_name := NEW.fields->'name'->>'value';

    -- RULE 1: Every blob must have composition and name
    IF v_composition IS NULL OR v_composition = '' THEN
        RAISE EXCEPTION 'GOVERNANCE: blob rejected — missing composition field';
    END IF;
    IF v_name IS NULL OR v_name = '' THEN
        RAISE EXCEPTION 'GOVERNANCE: blob rejected — missing name field';
    END IF;

    -- RULE 2: Composition must be known (skip for composition and field-type self-enrollment)
    IF v_composition NOT IN ('composition', 'field-type') THEN
        SELECT EXISTS(
            SELECT 1 FROM substrate.blob
            WHERE fields->'composition'->>'value' = 'composition'
            AND fields->'name'->>'value' = v_composition
        ) INTO v_known_comp;

        IF NOT v_known_comp THEN
            RAISE EXCEPTION 'GOVERNANCE: blob rejected — unknown composition "%". Enroll it first.', v_composition;
        END IF;
    END IF;

    -- RULE 3: No duplicate (composition, name) — on INSERT only
    IF TG_OP = 'INSERT' THEN
        SELECT unid INTO v_existing
        FROM substrate.blob
        WHERE fields->'composition'->>'value' = v_composition
        AND fields->'name'->>'value' = v_name
        LIMIT 1;

        IF v_existing IS NOT NULL THEN
            RAISE EXCEPTION 'GOVERNANCE: blob rejected — duplicate (%, %) already exists as %. Use UPDATE to mutate.',
                v_composition, v_name, v_existing;
        END IF;
    END IF;

    -- RULE 4: Subscriber must not be empty
    IF NEW.subscriber IS NULL OR array_length(NEW.subscriber, 1) IS NULL THEN
        NEW.subscriber := ARRAY['SYSTEM'];
    END IF;

    -- RULE 5: Tag chunk blobs as internal
    IF v_name LIKE 'chunk_%' AND v_composition = 'file' THEN
        NEW.fields := NEW.fields || jsonb_build_object(
            'internal', jsonb_build_object('type', 'boolean', 'value', true)
        );
    END IF;

    -- RULE 6: Validate field structure
    FOR v_field_key, v_field_val IN SELECT * FROM jsonb_each(NEW.fields)
    LOOP
        IF v_field_val IS NOT NULL AND jsonb_typeof(v_field_val) = 'object' THEN
            IF NOT (v_field_val ? 'type' AND v_field_val ? 'value') THEN
                RAISE WARNING 'GOVERNANCE: field "%" missing type/value structure', v_field_key;
            END IF;
        END IF;
    END LOOP;

    RETURN NEW;
END;
$function$
