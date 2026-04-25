-- New field type: compressed — blob content after compression
-- New composition: stream — a blob that describes how to move another blob somewhere

-- Force: compress — takes a blob, produces a compressed blob
CREATE OR REPLACE FUNCTION substrate.compress(
    p_unid UUID,
    p_algorithm TEXT DEFAULT 'zlib'
) RETURNS UUID AS $$
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
$$ LANGUAGE plpython3u;

-- Force: decompress — takes a compressed blob, returns original content
CREATE OR REPLACE FUNCTION substrate.decompress(p_unid UUID)
RETURNS BYTEA AS $$
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
$$ LANGUAGE plpython3u;

-- Force: emit — stream a blob to any endpoint via any protocol
-- The subscriber column tells you WHO. This function tells you HOW.
CREATE OR REPLACE FUNCTION substrate.emit(
    p_unid UUID,
    p_endpoint TEXT,         -- where: URL, path, address
    p_protocol TEXT DEFAULT 'http',  -- how: http, file, pg, s3, ws
    p_compress BOOLEAN DEFAULT true
) RETURNS TEXT AS $$
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
$$ LANGUAGE plpython3u;

-- Force: ingest_compressed — ingest a file, compress it, store as one blob
-- This solves the 234MB Claude binary problem: compress first, then store
CREATE OR REPLACE FUNCTION substrate.ingest_compressed(
    p_path TEXT,
    p_algorithm TEXT DEFAULT 'zlib',
    p_subscriber TEXT[] DEFAULT '{SYSTEM}'
) RETURNS UUID AS $$
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
$$ LANGUAGE plpython3u;

-- Force: memfd_exec_compressed — decompress and run from memory, zero disk
CREATE OR REPLACE FUNCTION substrate.memfd_exec_compressed(
    p_unid UUID,
    p_argv TEXT[] DEFAULT '{}'
) RETURNS TEXT AS $$
import ctypes, os, subprocess, json, base64, zlib

row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_unid])

if not row:
    plpy.error(f'blob {p_unid} not found')

fields = row[0]['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

name = fields.get('name', {}).get('value', 'binary')
algorithm = fields.get('algorithm', {}).get('value', 'zlib')
compressed_b64 = fields.get('content', {}).get('value', '')

compressed = base64.b64decode(compressed_b64)

if algorithm == 'zlib':
    raw = zlib.decompress(compressed)
elif algorithm == 'gzip':
    import gzip
    raw = gzip.decompress(compressed)
else:
    return f'unknown algorithm: {algorithm}'

libc = ctypes.CDLL('libc.so.6')
libc.memfd_create.restype = ctypes.c_int
libc.memfd_create.argtypes = [ctypes.c_char_p, ctypes.c_uint]
fd = libc.memfd_create(name.encode(), 0)
if fd < 0:
    return 'memfd_create failed'

os.write(fd, raw)
os.lseek(fd, 0, os.SEEK_SET)
exe_path = f'/proc/{os.getpid()}/fd/{fd}'
cmd = [exe_path] + list(p_argv)

result = subprocess.run(cmd, capture_output=True, text=True, close_fds=False, timeout=30)
os.close(fd)
return result.stdout + result.stderr
$$ LANGUAGE plpython3u;
