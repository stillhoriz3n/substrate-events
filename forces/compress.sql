CREATE OR REPLACE FUNCTION substrate.compress(p_unid uuid, p_algorithm text DEFAULT 'zlib'::text)
 RETURNS uuid
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

content_b64 = fields.get('content', {}).get('value', '')
if not content_b64:
    plpy.error('blob has no content to compress')

raw = base64.b64decode(content_b64)
original_size = len(raw)

if p_algorithm == 'zlib':
    compressed = zlib.compress(raw, 9)
elif p_algorithm == 'gzip':
    import gzip
    compressed = gzip.compress(raw, compresslevel=9)
else:
    plpy.error(f'unknown compression algorithm: {p_algorithm}')

compressed_b64 = base64.b64encode(compressed).decode('ascii')
compressed_size = len(compressed)
ratio = round(compressed_size / original_size * 100, 1)

name = fields.get('name', {}).get('value', 'unnamed')

plan = plpy.prepare("""
    INSERT INTO substrate.blob (fields, subscriber)
    VALUES (
        jsonb_build_object(
            'composition',    jsonb_build_object('type', 'utf8', 'value', 'file'),
            'name',           jsonb_build_object('type', 'utf8', 'value', $1),
            'content',        jsonb_build_object('type', 'compressed', 'value', ''),
            'algorithm',      jsonb_build_object('type', 'utf8', 'value', $2),
            'original_size',  jsonb_build_object('type', 'integer', 'value', $3),
            'compressed_size',jsonb_build_object('type', 'integer', 'value', $4),
            'source',         jsonb_build_object('type', 'reference', 'value', $5),
            'ratio',          jsonb_build_object('type', 'utf8', 'value', $6)
        ),
        '{SYSTEM}'
    ) RETURNING unid
""", ["text", "text", "int", "int", "text", "text"])

crow = plpy.execute(plan, [
    f'{name}.{p_algorithm}',
    p_algorithm,
    original_size,
    compressed_size,
    str(p_unid),
    f'{ratio}%'
])
comp_unid = crow[0]['unid']

# Update with compressed content
cp2 = plpy.prepare(
    "UPDATE substrate.blob SET fields = jsonb_set(fields, '{content,value}', to_jsonb($1::text)) WHERE unid = $2",
    ["text", "uuid"]
)
plpy.execute(cp2, [compressed_b64, comp_unid])

# Signal
sp = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'compress', jsonb_build_object(
        'source', $2, 'algorithm', $3,
        'original_size', $4, 'compressed_size', $5, 'ratio', $6
    ), '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text", "text", "int", "int", "text"])
plpy.execute(sp, [comp_unid, str(p_unid), p_algorithm, original_size, compressed_size, f'{ratio}%'])

plpy.notice(f'Compressed {name}: {original_size} -> {compressed_size} ({ratio}%)')
return comp_unid
$function$
