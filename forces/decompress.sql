CREATE OR REPLACE FUNCTION substrate.decompress(p_unid uuid)
 RETURNS bytea
 LANGUAGE plpython3u
AS $function$
import base64, zlib, json

row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_unid])

if not row:
    plpy.error(f'blob {p_unid} not found')

fields = row[0]['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

compressed_b64 = fields.get('content', {}).get('value', '')
algorithm = fields.get('algorithm', {}).get('value', 'zlib')

compressed = base64.b64decode(compressed_b64)

if algorithm == 'zlib':
    return zlib.decompress(compressed)
elif algorithm == 'gzip':
    import gzip
    return gzip.decompress(compressed)
else:
    plpy.error(f'unknown algorithm: {algorithm}')
$function$
