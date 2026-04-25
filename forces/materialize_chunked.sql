CREATE OR REPLACE FUNCTION substrate.materialize_chunked(p_package_unid uuid)
 RETURNS text
 LANGUAGE plpython3u
AS $function$
import os, json, base64

row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_package_unid])

if not row:
    plpy.error(f'package blob {p_package_unid} not found')

fields = row[0]['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

path = fields.get('path', {}).get('value')
perm = fields.get('permission', {}).get('value', '755')
chunk_refs_raw = fields.get('chunk_refs', {}).get('value', '[]')
chunk_unids = json.loads(chunk_refs_raw)

os.makedirs(os.path.dirname(path), exist_ok=True)

with open(path, 'wb') as f:
    for chunk_unid in chunk_unids:
        crow = plpy.execute(plpy.prepare(
            "SELECT fields->'content'->>'value' as content FROM substrate.blob WHERE unid = $1::uuid",
            ["text"]
        ), [chunk_unid])
        if crow:
            chunk_bytes = base64.b64decode(crow[0]['content'])
            f.write(chunk_bytes)

os.chmod(path, int(perm, 8))
return path
$function$
