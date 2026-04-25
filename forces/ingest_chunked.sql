CREATE OR REPLACE FUNCTION substrate.ingest_chunked(p_path text, p_chunk_size integer DEFAULT 100000000, p_subscriber text[] DEFAULT '{SYSTEM}'::text[])
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
import base64, os, math

path = p_path
name = os.path.basename(path)
ext = os.path.splitext(name)[1].lower()

type_map = {
    '.exe': 'executable', '.bin': 'executable',
}
content_type = type_map.get(ext, 'executable')

file_size = os.path.getsize(path)
num_chunks = math.ceil(file_size / p_chunk_size)

try:
    perm = oct(os.stat(path).st_mode)[-3:]
except:
    perm = '755'

# Create the parent package blob (no content, references chunks)
plan = plpy.prepare("""
    INSERT INTO substrate.blob (fields, subscriber)
    VALUES (
        jsonb_build_object(
            'composition', jsonb_build_object('type', 'utf8', 'value', 'package'),
            'name',        jsonb_build_object('type', 'utf8', 'value', $1),
            'path',        jsonb_build_object('type', 'path', 'value', $2),
            'content_type',jsonb_build_object('type', 'utf8', 'value', $3),
            'size',        jsonb_build_object('type', 'integer', 'value', $4),
            'chunks',      jsonb_build_object('type', 'integer', 'value', $5),
            'permission',  jsonb_build_object('type', 'permission', 'value', $6)
        ),
        $7
    ) RETURNING unid
""", ["text", "text", "text", "int", "int", "text", "text[]"])

row = plpy.execute(plan, [name, path, content_type, file_size, num_chunks, perm, list(p_subscriber)])
parent_unid = row[0]['unid']

chunk_unids = []
with open(path, 'rb') as f:
    for i in range(num_chunks):
        chunk_data = f.read(p_chunk_size)
        b64 = base64.b64encode(chunk_data).decode('ascii')
        chunk_size = len(chunk_data)

        cp = plpy.prepare("""
            INSERT INTO substrate.blob (fields, subscriber)
            VALUES (
                jsonb_build_object(
                    'composition', jsonb_build_object('type', 'utf8', 'value', 'file'),
                    'name',        jsonb_build_object('type', 'utf8', 'value', $1),
                    'path',        jsonb_build_object('type', 'path', 'value', $2),
                    'content',     jsonb_build_object('type', 'base64', 'value', ''),
                    'size',        jsonb_build_object('type', 'integer', 'value', $3),
                    'chunk_index', jsonb_build_object('type', 'integer', 'value', $4),
                    'parent',      jsonb_build_object('type', 'reference', 'value', $5)
                ),
                $6
            ) RETURNING unid
        """, ["text", "text", "int", "int", "text", "text[]"])

        crow = plpy.execute(cp, [
            f'{name}.chunk.{i}',
            f'{path}.chunk.{i}',
            chunk_size,
            i,
            str(parent_unid),
            list(p_subscriber)
        ])
        chunk_unid = crow[0]['unid']

        # Update with actual content
        cp2 = plpy.prepare(
            "UPDATE substrate.blob SET fields = jsonb_set(fields, '{content,value}', to_jsonb($1::text)) WHERE unid = $2",
            ["text", "uuid"]
        )
        plpy.execute(cp2, [b64, chunk_unid])

        chunk_unids.append(str(chunk_unid))
        plpy.notice(f'Chunk {i+1}/{num_chunks}: {chunk_size} bytes -> blob {chunk_unid}')

# Store chunk references on parent
import json
up = plpy.prepare(
    "UPDATE substrate.blob SET fields = jsonb_set(fields, '{chunk_refs}', jsonb_build_object('type', 'utf8', 'value', $1::text)) WHERE unid = $2",
    ["text", "uuid"]
)
plpy.execute(up, [json.dumps(chunk_unids), parent_unid])

# Signal
sp = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'ingest', jsonb_build_object('path', $2, 'size', $3, 'chunks', $4),
            '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text", "int", "int"])
plpy.execute(sp, [parent_unid, path, file_size, num_chunks])

return parent_unid
$function$
