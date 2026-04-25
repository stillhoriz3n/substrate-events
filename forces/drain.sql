CREATE OR REPLACE FUNCTION substrate.drain(p_subscription_unid uuid, p_max_batch integer DEFAULT 10)
 RETURNS integer
 LANGUAGE plpython3u
AS $function$
import json

# Get subscription details
sub_row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_subscription_unid])

if not sub_row:
    return 0

sub_fields = sub_row[0]['fields']
if isinstance(sub_fields, str):
    sub_fields = json.loads(sub_fields)

endpoint = sub_fields.get('endpoint', {}).get('value', '')
protocol = sub_fields.get('protocol', {}).get('value', 'http')
compress_algo = sub_fields.get('compress', {}).get('value', None)
rate_limit = sub_fields.get('rate_limit', {}).get('value', None)

# Get pipe state
ps_row = plpy.execute(plpy.prepare("""
    SELECT unid FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'pipe_state'
    AND fields->'subscription'->>'value' = $1
    LIMIT 1
""", ["text"]), [str(p_subscription_unid)])

pipe_state_unid = ps_row[0]['unid'] if ps_row else None

# Get queued emissions, ordered by priority DESC
queued = plpy.execute(plpy.prepare("""
    SELECT unid, fields->'blob'->>'value' as blob_unid,
           (fields->'priority'->>'value')::int as priority
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'emission'
    AND fields->'subscription'->>'value' = $1
    AND fields->'state'->>'value' = 'queued'
    ORDER BY (fields->'priority'->>'value')::int DESC,
             fields->'queued_at'->>'value' ASC
    LIMIT $2
""", ["text", "int"]), [str(p_subscription_unid), p_max_batch])

drained = 0
for emission in queued:
    # Check rate limit
    if pipe_state_unid and rate_limit:
        allowed = plpy.execute(plpy.prepare(
            "SELECT substrate.throttle($1, $2) as ok", ["uuid", "text"]
        ), [pipe_state_unid, rate_limit])
        if not allowed[0]['ok']:
            break

    blob_unid = emission['blob_unid']

    # Compress if needed
    source_unid = blob_unid
    if compress_algo:
        try:
            comp_row = plpy.execute(plpy.prepare(
                "SELECT substrate.compress($1::uuid, $2) as unid", ["text", "text"]
            ), [blob_unid, compress_algo])
            source_unid = comp_row[0]['unid']
        except:
            pass

    # Emit
    try:
        plpy.execute(plpy.prepare(
            "SELECT substrate.emit($1::uuid, $2, $3, false)", ["text", "text", "text"]
        ), [str(source_unid), endpoint, protocol])

        # Mark emission as delivered
        plpy.execute(plpy.prepare("""
            UPDATE substrate.blob SET fields = fields
                || jsonb_build_object(
                    'state', jsonb_build_object('type', 'utf8', 'value', 'delivered'),
                    'delivered_at', jsonb_build_object('type', 'timestamp', 'value', now()::text)
                )
            WHERE unid = $1
        """, ["uuid"]), [emission['unid']])
        drained += 1

    except Exception as e:
        # Mark emission as failed, increment attempts
        plpy.execute(plpy.prepare("""
            UPDATE substrate.blob SET fields = fields
                || jsonb_build_object(
                    'state', jsonb_build_object('type', 'utf8', 'value', 'failed'),
                    'last_error', jsonb_build_object('type', 'utf8', 'value', $1),
                    'attempts', jsonb_build_object('type', 'integer', 'value',
                        COALESCE((fields->'attempts'->>'value')::int, 0) + 1)
                )
            WHERE unid = $2
        """, ["text", "uuid"]), [str(e)[:500], emission['unid']])

        # Increment failure count on pipe state
        if pipe_state_unid:
            plpy.execute(plpy.prepare("""
                UPDATE substrate.blob SET fields = jsonb_set(
                    fields, '{failures,value}',
                    to_jsonb((COALESCE((fields->'failures'->>'value')::int, 0) + 1))
                ) WHERE unid = $1
            """, ["uuid"]), [pipe_state_unid])

# Update queue depth
if drained > 0 and pipe_state_unid:
    plpy.execute(plpy.prepare("""
        UPDATE substrate.blob SET fields = jsonb_set(
            fields, '{queue_depth,value}',
            to_jsonb(GREATEST(0, COALESCE((fields->'queue_depth'->>'value')::int, 0) - $1))
        ) WHERE unid = $2
    """, ["int", "uuid"]), [drained, pipe_state_unid])

return drained
$function$
