CREATE OR REPLACE FUNCTION substrate.ingest(p_path text, p_subscriber text[] DEFAULT '{SYSTEM}'::text[])
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
import base64, os

path = p_path
name = os.path.basename(path)
ext = os.path.splitext(name)[1].lower()

type_map = {
    '.exe': 'executable', '.bin': 'executable',
    '.wasm': 'wasm',
    '.png': 'png', '.jpg': 'jpeg', '.jpeg': 'jpeg',
    '.pdf': 'pdf', '.pem': 'pem',
    '.zip': 'archive', '.tar': 'archive', '.gz': 'archive',
}
content_type = type_map.get(ext, 'base64')

with open(path, 'rb') as f:
    raw = f.read()

b64 = base64.b64encode(raw).decode('ascii')
size = len(raw)

try:
    perm = oct(os.stat(path).st_mode)[-3:]
except:
    perm = '755'

plan = plpy.prepare("""
    INSERT INTO substrate.blob (fields, subscriber)
    VALUES (
        jsonb_build_object(
            'composition', jsonb_build_object('type', 'utf8', 'value', 'file'),
            'name',        jsonb_build_object('type', 'utf8', 'value', $1),
            'path',        jsonb_build_object('type', 'path', 'value', $2),
            'content',     jsonb_build_object('type', $3, 'value', ''),
            'size',        jsonb_build_object('type', 'integer', 'value', $4),
            'permission',  jsonb_build_object('type', 'permission', 'value', $5)
        ),
        $6
    ) RETURNING unid
""", ["text", "text", "text", "int", "text", "text[]"])

row = plpy.execute(plan, [name, path, content_type, size, perm, list(p_subscriber)])
unid = row[0]['unid']

plan2 = plpy.prepare(
    "UPDATE substrate.blob SET fields = jsonb_set(fields, '{content,value}', to_jsonb($1::text)) WHERE unid = $2",
    ["text", "uuid"]
)
plpy.execute(plan2, [b64, unid])

plan3 = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'ingest', jsonb_build_object('path', $2, 'size', $3), '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text", "int"])
plpy.execute(plan3, [unid, path, size])

return unid
$function$
