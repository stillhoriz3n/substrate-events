CREATE OR REPLACE FUNCTION substrate.enqueue(p_subscription_unid uuid, p_blob_unid uuid, p_priority integer DEFAULT 0)
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
import json

plan = plpy.prepare("""
    INSERT INTO substrate.blob (fields, subscriber)
    VALUES (
        jsonb_build_object(
            'composition',    jsonb_build_object('type', 'utf8', 'value', 'emission'),
            'subscription',   jsonb_build_object('type', 'reference', 'value', $1),
            'blob',           jsonb_build_object('type', 'reference', 'value', $2),
            'priority',       jsonb_build_object('type', 'integer', 'value', $3),
            'state',          jsonb_build_object('type', 'utf8', 'value', 'queued'),
            'queued_at',      jsonb_build_object('type', 'timestamp', 'value', now()::text),
            'attempts',       jsonb_build_object('type', 'integer', 'value', 0)
        ),
        '{SYSTEM}'
    ) RETURNING unid
""", ["text", "text", "int"])

row = plpy.execute(plan, [str(p_subscription_unid), str(p_blob_unid), p_priority])

# Increment queue depth on pipe_state
plpy.execute(plpy.prepare("""
    UPDATE substrate.blob SET fields = jsonb_set(
        fields, '{queue_depth,value}',
        to_jsonb((COALESCE((fields->'queue_depth'->>'value')::int, 0) + 1))
    )
    WHERE fields->'composition'->>'value' = 'pipe_state'
    AND fields->'subscription'->>'value' = $1
""", ["text"]), [str(p_subscription_unid)])

return row[0]['unid']
$function$
