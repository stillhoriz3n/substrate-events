CREATE OR REPLACE FUNCTION substrate.constrain(p_composition text, p_name text)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json

unique_compositions = ['composition', 'field_type', 'field-type', 'principal', 'package', 'kernel', 'extension']

if p_composition not in unique_compositions:
    return json.dumps({'allowed': True, 'reason': f'{p_composition} is not uniqueness-constrained'})

existing = plpy.execute(plpy.prepare("""
    SELECT unid, fields->'name'->>'value' as name,
           fields->'description'->>'value' as description
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = $1
    AND fields->'name'->>'value' = $2
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
""", ["text", "text"]), [p_composition, p_name])

if existing:
    return json.dumps({
        'allowed': False,
        'reason': f'{p_composition} named "{p_name}" already exists',
        'existing_unid': str(existing[0]['unid']),
        'existing_description': existing[0]['description']
    })

return json.dumps({'allowed': True})
$function$
