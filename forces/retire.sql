CREATE OR REPLACE FUNCTION substrate.retire(p_unid uuid)
 RETURNS void
 LANGUAGE plpython3u
AS $function$

plpy.execute(plpy.prepare("""
    UPDATE substrate.blob SET
        retired_at = now(),
        fields = fields || jsonb_build_object(
            'state', jsonb_build_object('type', 'utf8', 'value', 'retired')
        )
    WHERE unid = $1
    AND retired_at IS NULL
""", ["uuid"]), [p_unid])

sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'retire', '{}', '00000000-0000-0000-0000-000000000001')
""", ["uuid"])
plpy.execute(sig, [p_unid])
$function$
