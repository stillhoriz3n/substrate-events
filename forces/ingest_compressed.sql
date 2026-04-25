CREATE OR REPLACE FUNCTION substrate.ingest_compressed(p_path text, p_algorithm text DEFAULT 'zlib'::text, p_subscriber text[] DEFAULT '{SYSTEM}'::text[])
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
import base64, zlib, os, json

path = p_path
name = os.path.basename(path)

with open(path, 'rb') as f:
    raw = f.read()

original_size = len(raw)

if p_algorithm == 'zlib':
    compressed = zlib.compress(raw, 9)
elif p_algorithm == 'gzip':
    import gzip
    compressed = gzip.compress(raw, compresslevel=9)
else:
    plpy.error(f'unknown algorithm: {p_algorithm}')

compressed_b64 = base64.b64encode(compressed).decode('ascii')
compressed_size = len(compressed)
ratio = round(compressed_size / original_size * 100, 1)

try:
    perm = oct(os.stat(path).st_mode)[-3:]
except:
    perm = '755'

plan = plpy.prepare("""
    INSERT INTO substrate.blob (fields, subscriber)
    VALUES (
        jsonb_build_object(
            'composition',    jsonb_build_object('type', 'utf8', 'value', 'file'),
            'name',           jsonb_build_object('type', 'utf8', 'value', $1),
            'path',           jsonb_build_object('type', 'path', 'value', $2),
            'content',        jsonb_build_object('type', 'compressed', 'value', ''),
            'algorithm',      jsonb_build_object('type', 'utf8', 'value', $3),
            'original_size',  jsonb_build_object('type', 'integer', 'value', $4),
            'compressed_size',jsonb_build_object('type', 'integer', 'value', $5),
            'ratio',          jsonb_build_object('type', 'utf8', 'value', $6),
            'permission',     jsonb_build_object('type', 'permission', 'value', $7)
        ),
        $8
    ) RETURNING unid
""", ["text", "text", "text", "int", "int", "text", "text", "text[]"])

row = plpy.execute(plan, [name, path, p_algorithm, original_size, compressed_size, f'{ratio}%', perm, list(p_subscriber)])
unid = row[0]['unid']

cp2 = plpy.prepare(
    "UPDATE substrate.blob SET fields = jsonb_set(fields, '{content,value}', to_jsonb($1::text)) WHERE unid = $2",
    ["text", "uuid"]
)
plpy.execute(cp2, [compressed_b64, unid])

sp = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'ingest', jsonb_build_object('path', $2, 'original_size', $3, 'compressed_size', $4, 'algorithm', $5),
            '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text", "int", "int", "text"])
plpy.execute(sp, [unid, path, original_size, compressed_size, p_algorithm])

plpy.notice(f'Ingested {name}: {original_size} -> {compressed_size} ({ratio}%)')
return unid
$function$
