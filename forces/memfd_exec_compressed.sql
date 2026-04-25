CREATE OR REPLACE FUNCTION substrate.memfd_exec_compressed(p_unid uuid, p_argv text[] DEFAULT '{}'::text[])
 RETURNS text
 LANGUAGE plpython3u
AS $function$
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
$function$
