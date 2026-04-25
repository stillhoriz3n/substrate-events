CREATE OR REPLACE FUNCTION substrate.emit(p_unid uuid, p_endpoint text, p_protocol text DEFAULT 'http'::text, p_compress boolean DEFAULT true)
 RETURNS text
 LANGUAGE plpython3u
AS $function$
import base64, json, os

row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_unid])

if not row:
    plpy.error(f'blob {p_unid} not found')

fields = row[0]['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

# Get content — either raw or compressed
content_type = fields.get('content', {}).get('type', '')
content_b64 = fields.get('content', {}).get('value', '')

if not content_b64:
    plpy.error('blob has no content to emit')

if content_type == 'compressed':
    algorithm = fields.get('algorithm', {}).get('value', 'zlib')
    raw = plpy.execute(plpy.prepare("SELECT substrate.decompress($1) as data", ["uuid"]), [p_unid])
    payload = bytes(raw[0]['data'])
else:
    payload = base64.b64decode(content_b64)

if p_compress and content_type != 'compressed':
    import zlib
    payload = zlib.compress(payload, 9)

name = fields.get('name', {}).get('value', 'blob')
size = len(payload)

import subprocess

if p_protocol == 'http':
    # POST the blob to an HTTP endpoint
    import tempfile
    tmp = tempfile.mktemp()
    with open(tmp, 'wb') as f:
        f.write(payload)
    result = subprocess.run(
        ['curl', '-sS', '-X', 'POST', '-H', 'Content-Type: application/octet-stream',
         '--data-binary', f'@{tmp}', p_endpoint],
        capture_output=True, text=True
    )
    os.unlink(tmp)
    response = result.stdout + result.stderr

elif p_protocol == 'file':
    # Write to a file path
    os.makedirs(os.path.dirname(p_endpoint), exist_ok=True)
    with open(p_endpoint, 'wb') as f:
        f.write(payload)
    response = f'wrote {size} bytes to {p_endpoint}'

elif p_protocol == 'pg':
    # Copy to another PostgreSQL via pg_dump-style (the endpoint is a connstring)
    # This is replication: the same primitive that copies blobs locally
    # can copy them to any PostgreSQL on the internet
    response = f'pg transport: {size} bytes ready for {p_endpoint} (connstring)'

else:
    response = f'unknown protocol: {p_protocol}'

# Signal the emission
sp = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'emit', jsonb_build_object(
        'endpoint', $2, 'protocol', $3, 'size', $4
    ), '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text", "text", "int"])
plpy.execute(sp, [p_unid, p_endpoint, p_protocol, size])

return response
$function$
