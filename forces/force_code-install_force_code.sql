CREATE OR REPLACE FUNCTION substrate.install_force_code(p_force_name text)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json

# Find the latest force_code blob for this force
rows = plpy.execute(plpy.prepare("""
    SELECT unid, fields FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'force_code'
    AND fields->'force_name'->>'value' = $1
    AND retired_at IS NULL
    ORDER BY ordinal DESC LIMIT 1
""", ["text"]), [p_force_name])

if not rows:
    return json.dumps({'error': 'no force_code for ' + p_force_name})

f = rows[0]['fields']
if isinstance(f, str):
    f = json.loads(f)
sql = f.get('body', {}).get('value', '')

if not sql:
    return json.dumps({'error': 'force_code body is empty'})

try:
    plpy.execute(sql)
    return json.dumps({'force':'install_force_code','action':'installed','force_name':p_force_name,'sql_length':len(sql)})
except Exception as e:
    return json.dumps({'force':'install_force_code','error':str(e)[:300],'force_name':p_force_name})
$function$
