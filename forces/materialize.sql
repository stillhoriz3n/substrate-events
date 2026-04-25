CREATE OR REPLACE FUNCTION substrate.materialize(p_unid uuid)
 RETURNS text
 LANGUAGE plpython3u
AS $function$
import base64, os

row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_unid])

if not row:
    plpy.error(f'blob {p_unid} not found')

fields = row[0]['fields']
import json
if isinstance(fields, str):
    fields = json.loads(fields)

path = fields.get('path', {}).get('value')
content = fields.get('content', {}).get('value')

if not path or not content:
    plpy.error('blob has no path or content field')

os.makedirs(os.path.dirname(path), exist_ok=True)
raw = base64.b64decode(content)
with open(path, 'wb') as f:
    f.write(raw)

perm = fields.get('permission', {}).get('value', '755')
os.chmod(path, int(perm, 8))

return path
$function$
