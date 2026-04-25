CREATE OR REPLACE FUNCTION substrate.gc(p_grace interval DEFAULT '7 days'::interval, p_mode text DEFAULT 'purge'::text)
 RETURNS jsonb
 LANGUAGE sql
AS $function$ SELECT substrate.reap(p_grace, p_mode, false) $function$
