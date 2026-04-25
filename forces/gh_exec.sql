CREATE OR REPLACE FUNCTION substrate.gh_exec(p_args text[], p_timeout integer DEFAULT 30)
 RETURNS text
 LANGUAGE plpython3u
AS $function$
import ctypes, os, subprocess, json, base64, zlib

# Find the gh blob
gh_row = plpy.execute("""
    SELECT unid, fields FROM substrate.blob
    WHERE fields->'name'->>'value' = 'gh'
    AND fields->'content'->>'type' = 'compressed'
    LIMIT 1
""")

if not gh_row:
    plpy.error('gh binary not found in substrate — run substrate.upload() to ingest it')

fields = gh_row[0]['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

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

# memfd
libc = ctypes.CDLL('libc.so.6')
libc.memfd_create.restype = ctypes.c_int
libc.memfd_create.argtypes = [ctypes.c_char_p, ctypes.c_uint]
fd = libc.memfd_create(b'gh', 0)
if fd < 0:
    return 'memfd_create failed'

os.write(fd, raw)
os.lseek(fd, 0, os.SEEK_SET)
exe_path = f'/proc/{os.getpid()}/fd/{fd}'

# Get the GitHub token from database setting
token_row = plpy.execute("SELECT current_setting('substrate.gh_token', true) as token")
token = token_row[0]['token'] if token_row and token_row[0]['token'] else ''

# Build environment — inherit current, add GH_TOKEN
env = dict(os.environ)
if token:
    env['GH_TOKEN'] = token
    env['GITHUB_TOKEN'] = token

cmd = [exe_path] + list(p_args)
result = subprocess.run(cmd, capture_output=True, text=True, close_fds=False,
                        timeout=p_timeout, env=env)
os.close(fd)
return result.stdout + result.stderr
$function$
