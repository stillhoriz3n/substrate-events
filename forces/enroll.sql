CREATE OR REPLACE FUNCTION substrate.enroll(p_name text, p_sql text, p_description text DEFAULT ''::text)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json

fields = {
    'composition':  {'type': 'utf8', 'value': 'force'},
    'name':         {'type': 'utf8', 'value': p_name},
    'description':  {'type': 'utf8', 'value': p_description},
    'body':         {'type': 'utf8', 'value': p_sql},
    'enrolled_at':  {'type': 'timestamp', 'value': ''}
}

# Check if this force already exists
existing = plpy.execute(plpy.prepare("""
    SELECT unid FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'force'
    AND fields->'name'->>'value' = $1
""", ["text"]), [p_name])

if existing:
    # Update the existing force blob
    blob_unid = existing[0]['unid']
    plpy.execute(plpy.prepare("""
        UPDATE substrate.blob SET fields = fields
            || jsonb_build_object(
                'body', jsonb_build_object('type', 'utf8', 'value', $1),
                'description', jsonb_build_object('type', 'utf8', 'value', $2),
                'updated_at', jsonb_build_object('type', 'timestamp', 'value', now()::text)
            )
        WHERE unid = $3
    """, ["text", "text", "uuid"]), [p_sql, p_description, blob_unid])
    action = 'updated'
else:
    # Create the force blob
    row = plpy.execute(plpy.prepare("""
        INSERT INTO substrate.blob (fields, subscriber)
        VALUES ($1::jsonb, '{SYSTEM}')
        RETURNING unid
    """, ["text"]), [json.dumps(fields)])
    blob_unid = row[0]['unid']

    # Set enrolled_at
    plpy.execute(plpy.prepare("""
        UPDATE substrate.blob SET fields = jsonb_set(
            fields, '{enrolled_at,value}', to_jsonb(now()::text)
        ) WHERE unid = $1
    """, ["uuid"]), [blob_unid])
    action = 'enrolled'

# Execute the SQL — the force takes effect immediately
try:
    plpy.execute(p_sql)
    executed = True
except Exception as e:
    executed = False

# Radiate — push the diff to all peers
try:
    radiate_row = plpy.execute(plpy.prepare(
        "SELECT substrate.radiate($1) as result", ["uuid"]
    ), [blob_unid])
    radiated = json.loads(radiate_row[0]['result'])
except Exception as e:
    radiated = {'error': str(e)}

result = {
    'action': action,
    'force': p_name,
    'blob_unid': str(blob_unid),
    'executed': executed,
    'radiated': radiated
}

return json.dumps(result)
$function$
