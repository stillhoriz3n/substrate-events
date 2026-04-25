CREATE OR REPLACE FUNCTION substrate.memfd_exec_chunked(p_package_unid uuid, p_argv text[] DEFAULT '{}'::text[])
 RETURNS text
 LANGUAGE plpython3u
AS $function$
import ctypes, os, subprocess, json, base64

# Get parent blob
row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_package_unid])

if not row:
    plpy.error(f'package blob {p_package_unid} not found')

fields = row[0]['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

name = fields.get('name', {}).get('value', 'binary')
chunk_refs_raw = fields.get('chunk_refs', {}).get('value', '[]')
chunk_unids = json.loads(chunk_refs_raw)

# Create memfd
libc = ctypes.CDLL('libc.so.6')
libc.memfd_create.restype = ctypes.c_int
libc.memfd_create.argtypes = [ctypes.c_char_p, ctypes.c_uint]
fd = libc.memfd_create(name.encode(), 0)
if fd < 0:
    return 'memfd_create failed'

# Write chunks in order
for chunk_unid in chunk_unids:
    crow = plpy.execute(plpy.prepare(
        "SELECT fields->'content'->>'value' as content FROM substrate.blob WHERE unid = $1::uuid",
        ["text"]
    ), [chunk_unid])
    if crow:
        chunk_b64 = crow[0]['content']
        chunk_bytes = base64.b64decode(chunk_b64)
        os.write(fd, chunk_bytes)
        plpy.notice(f'Wrote chunk {chunk_unid}: {len(chunk_bytes)} bytes')

os.lseek(fd, 0, os.SEEK_SET)
exe_path = f'/proc/{os.getpid()}/fd/{fd}'
cmd = [exe_path] + list(p_argv)

result = subprocess.run(cmd, capture_output=True, text=True, close_fds=False, timeout=30)
os.close(fd)
return result.stdout + result.stderr
$function$
