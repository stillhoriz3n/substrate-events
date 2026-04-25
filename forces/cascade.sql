CREATE OR REPLACE FUNCTION substrate.cascade(p_blob_unid uuid)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json

results = {
    'source': str(p_blob_unid),
    'cascaded': [],
    'count': 0
}

source = plpy.execute(plpy.prepare(
    "SELECT fields->'composition'->>'value' as composition FROM substrate.blob WHERE unid = $1",
    ["uuid"]
), [p_blob_unid])

if not source:
    return json.dumps({'error': 'blob not found'})

composition = source[0]['composition']

dependents = plpy.execute(plpy.prepare("""
    SELECT unid,
           fields->'composition'->>'value' as composition,
           fields->'name'->>'value' as name
    FROM substrate.blob
    WHERE (
        -- pipe_state references subscription
        (fields->'subscription'->>'value' = $1)
        -- emission references source blob
        OR (fields->'source_blob'->>'value' = $1)
        -- reply references thread
        OR (fields->'thread'->>'value' = $1)
        -- any blob with a 'parent' reference
        OR (fields->'parent'->>'value' = $1)
    )
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
""", ["text"]), [str(p_blob_unid)])

for dep in dependents:
    try:
        plpy.execute(plpy.prepare(
            "SELECT substrate.retire($1)", ["uuid"]
        ), [dep['unid']])
        results['cascaded'].append({
            'unid': str(dep['unid']),
            'composition': dep['composition'],
            'name': dep['name']
        })
        results['count'] += 1
    except Exception as e:
        results['cascaded'].append({
            'unid': str(dep['unid']),
            'error': str(e)
        })

sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'cascade', $2::jsonb, '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text"])
plpy.execute(sig, [p_blob_unid, json.dumps(results)])

return json.dumps(results)
$function$
