CREATE OR REPLACE FUNCTION substrate.ack(p_emission_unid uuid)
 RETURNS void
 LANGUAGE plpython3u
AS $function$
from datetime import datetime

plpy.execute(plpy.prepare("""
    UPDATE substrate.blob SET fields = fields
        || jsonb_build_object(
            'state', jsonb_build_object('type', 'utf8', 'value', 'acknowledged'),
            'acked_at', jsonb_build_object('type', 'timestamp', 'value', $1)
        )
    WHERE unid = $2
    AND fields->'composition'->>'value' = 'emission'
""", ["text", "uuid"]), [datetime.utcnow().isoformat() + 'Z', p_emission_unid])

sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'ack', '{}', '00000000-0000-0000-0000-000000000001')
""", ["uuid"])
plpy.execute(sig, [p_emission_unid])
$function$
