CREATE OR REPLACE FUNCTION substrate.ensure_pipe_state(p_subscription_unid uuid)
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
import json

existing = plpy.execute(plpy.prepare("""
    SELECT unid FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'pipe_state'
    AND fields->'subscription'->>'value' = $1
    AND (fields->'state' IS NULL OR fields->'state'->>'value' != 'retired')
    LIMIT 1
""", ["text"]), [str(p_subscription_unid)])

if existing:
    return existing[0]['unid']

plan = plpy.prepare("""
    INSERT INTO substrate.blob (fields, subscriber)
    VALUES (
        jsonb_build_object(
            'composition',   jsonb_build_object('type', 'utf8', 'value', 'pipe_state'),
            'subscription',  jsonb_build_object('type', 'reference', 'value', $1),
            'emit_count',    jsonb_build_object('type', 'integer', 'value', 0),
            'window_start',  jsonb_build_object('type', 'timestamp', 'value', now()::text),
            'queue_depth',   jsonb_build_object('type', 'integer', 'value', 0),
            'dedup_hashes',  jsonb_build_object('type', 'json', 'value', '{}'),
            'last_emit',     jsonb_build_object('type', 'timestamp', 'value', ''),
            'last_ack',      jsonb_build_object('type', 'timestamp', 'value', ''),
            'failures',      jsonb_build_object('type', 'integer', 'value', 0),
            'status',        jsonb_build_object('type', 'utf8', 'value', 'open')
        ),
        '{SYSTEM}'
    ) RETURNING unid
""", ["text"])

row = plpy.execute(plan, [str(p_subscription_unid)])
return row[0]['unid']
$function$
