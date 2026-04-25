-- Update force_conservation to allow the reaper to redirect signal references
CREATE OR REPLACE FUNCTION substrate.force_conservation()
RETURNS TRIGGER AS $$
BEGIN
    -- The reaper is the only force allowed to modify signals
    -- It redirects blob_unid to REAPED sentinel before deleting rows
    IF TG_OP = 'UPDATE' AND current_setting('substrate.reaper_active', true) = 'true' THEN
        -- Only allow changing blob_unid to the REAPED sentinel
        IF NEW.blob_unid = '00000000-0000-0000-0000-000000000000' THEN
            RETURN NEW;
        END IF;
    END IF;
    RAISE EXCEPTION 'signals are append-only: cannot % signal %', TG_OP, OLD.id;
END;
$$ LANGUAGE plpgsql;
