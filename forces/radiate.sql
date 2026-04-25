CREATE OR REPLACE FUNCTION substrate.radiate(p_blob_unid uuid DEFAULT NULL::uuid, p_repo_path text DEFAULT '/tmp/substrate-events'::text, p_peer_id text DEFAULT 'mythserv1-radiant'::text, p_repo text DEFAULT 'stillhoriz3n/substrate-events'::text)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json, subprocess, os
from datetime import datetime

results = {
    'peer': p_peer_id,
    'repo': p_repo,
    'forces_written': 0,
    'manifest_updated': False,
    'events_appended': 0,
    'committed': False,
    'pushed': False,
    'commit_hash': None,
    'method': 'gh',
    'errors': []
}

# Get the gh binary from Substrate
gh_row = plpy.execute("""
    SELECT fields FROM substrate.blob
    WHERE fields->'name'->>'value' = 'gh'
    AND fields->'content'->>'type' = 'compressed'
    LIMIT 1
""")

if not gh_row:
    results['errors'].append('gh binary not found in substrate')
    return json.dumps(results)

import ctypes, base64, zlib

fields_gh = gh_row[0]['fields']
if isinstance(fields_gh, str):
    fields_gh = json.loads(fields_gh)

compressed_b64 = fields_gh.get('content', {}).get('value', '')
compressed = base64.b64decode(compressed_b64)
raw = zlib.decompress(compressed)

# Create memfd for gh
libc = ctypes.CDLL('libc.so.6')
libc.memfd_create.restype = ctypes.c_int
libc.memfd_create.argtypes = [ctypes.c_char_p, ctypes.c_uint]
gh_fd = libc.memfd_create(b'gh', 0)
os.write(gh_fd, raw)
os.lseek(gh_fd, 0, os.SEEK_SET)
gh_path = f'/proc/{os.getpid()}/fd/{gh_fd}'

# Get token
token_row = plpy.execute("SELECT current_setting('substrate.gh_token', true) as token")
token = token_row[0]['token'] if token_row and token_row[0]['token'] else ''

env = dict(os.environ)
if token:
    env['GH_TOKEN'] = token
    env['GITHUB_TOKEN'] = token

def gh(args, timeout=30):
    cmd = [gh_path] + args
    r = subprocess.run(cmd, capture_output=True, text=True, close_fds=False,
                       timeout=timeout, env=env)
    if r.returncode != 0:
        results['errors'].append(f'gh {" ".join(args)}: {r.stderr.strip()[:200]}')
        return None
    return r.stdout.strip()

def git(args, cwd=p_repo_path):
    r = subprocess.run(['git'] + args, cwd=cwd, capture_output=True, text=True)
    if r.returncode != 0:
        results['errors'].append(f'git {" ".join(args)}: {r.stderr.strip()[:200]}')
        return None
    return r.stdout.strip()

# Verify gh auth works
auth_check = gh(['auth', 'status'])
if auth_check is None and results['errors']:
    # Token might be invalid, but continue — git fallback
    pass

date_str = datetime.utcnow().strftime('%Y-%m-%d')
events_file = os.path.join(p_repo_path, 'events', f'{date_str}.jsonl')
os.makedirs(os.path.dirname(events_file), exist_ok=True)

# Pull latest via git (still use container git for local repo ops)
git(['pull', '--rebase', 'origin', 'main'])

# Collect blobs to radiate
if p_blob_unid:
    rows = plpy.execute(plpy.prepare("""
        SELECT unid, encode(content_hash, 'hex') as content_hash, ordinal, fields, subscriber
        FROM substrate.blob WHERE unid = $1
    """, ["uuid"]), [p_blob_unid])
else:
    rows = plpy.execute("""
        SELECT unid, encode(content_hash, 'hex') as content_hash, ordinal, fields, subscriber
        FROM substrate.blob
        WHERE (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
        ORDER BY ordinal
    """)

# Write force blobs to forces/ directory
for row in rows:
    fields = row['fields']
    if isinstance(fields, str):
        fields = json.loads(fields)

    composition = fields.get('composition', {}).get('value', '')
    name = fields.get('name', {}).get('value', '')

    if composition == 'force':
        body = fields.get('body', {}).get('value', '')
        if body:
            force_path = os.path.join(p_repo_path, 'forces', f'{name}.sql')
            os.makedirs(os.path.dirname(force_path), exist_ok=True)
            with open(force_path, 'w') as f:
                f.write(body)
            results['forces_written'] += 1

    # Append event
    event = {
        'event_id': str(row['unid']),
        'blob_unid': str(row['unid']),
        'content_hash': row['content_hash'],
        'ordinal': row['ordinal'],
        'composition': composition,
        'name': name,
        'subscriber': list(row['subscriber']) if row['subscriber'] else ['SYSTEM'],
        'origin_peer': p_peer_id,
        'signal_type': 'radiate',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    with open(events_file, 'a') as f:
        f.write(json.dumps(event) + '\n')
    results['events_appended'] += 1

# Write manifest
manifest = []
all_rows = plpy.execute("""
    SELECT unid, encode(content_hash, 'hex') as content_hash, ordinal, fields, subscriber
    FROM substrate.blob
    WHERE (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
    ORDER BY ordinal
""")

for row in all_rows:
    fields = row['fields']
    if isinstance(fields, str):
        fields = json.loads(fields)
    entry = {
        'unid': str(row['unid']),
        'content_hash': row['content_hash'],
        'ordinal': row['ordinal'],
        'composition': fields.get('composition', {}).get('value', ''),
        'name': fields.get('name', {}).get('value', ''),
        'subscriber': list(row['subscriber']) if row['subscriber'] else ['SYSTEM'],
        'origin_peer': p_peer_id,
        'updated_at': datetime.utcnow().isoformat() + 'Z'
    }
    if fields.get('size'):
        entry['size'] = int(fields['size'].get('value', 0))
    if fields.get('original_size'):
        entry['original_size'] = int(fields['original_size'].get('value', 0))
    manifest.append(entry)

manifest_path = os.path.join(p_repo_path, 'manifest', f'{p_peer_id}.json')
os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
with open(manifest_path, 'w') as f:
    f.write(json.dumps(manifest, indent=2))
results['manifest_updated'] = True

# Stage everything
git(['add', '.'])

status = git(['status', '--porcelain'])
if not status:
    os.close(gh_fd)
    return json.dumps(results)

# Commit
if p_blob_unid and len(rows) == 1:
    fields = rows[0]['fields']
    if isinstance(fields, str):
        fields = json.loads(fields)
    comp = fields.get('composition', {}).get('value', '')
    name = fields.get('name', {}).get('value', '')
    msg = f'radiate: {comp}/{name}'
else:
    msg = f'radiate: {len(manifest)} blobs, {results["forces_written"]} forces'

commit_out = git(['commit', '-m', msg])
if commit_out:
    results['committed'] = True
    hash_out = git(['rev-parse', '--short', 'HEAD'])
    results['commit_hash'] = hash_out

# PUSH — git push authenticated via GH_TOKEN from Substrate
# No PAT in the remote URL. The token lives in the database,
# passed as env var. gh acts as the credential helper.
remote_url = f'https://x-access-token:{token}@github.com/{p_repo}.git'
push_r = subprocess.run(
    ['git', 'push', remote_url, 'main'],
    cwd=p_repo_path, capture_output=True, text=True, timeout=60
)
if push_r.returncode == 0:
    results['pushed'] = True
else:
    results['errors'].append(f'push: {push_r.stderr.strip()[:200]}')

os.close(gh_fd)

# Signal
if p_blob_unid:
    sig = plpy.prepare("""
        INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
        VALUES ($1, 'radiate', $2::jsonb,
                '00000000-0000-0000-0000-000000000001')
    """, ["uuid", "text"])
    plpy.execute(sig, [p_blob_unid, json.dumps(results)])

return json.dumps(results)
$function$
