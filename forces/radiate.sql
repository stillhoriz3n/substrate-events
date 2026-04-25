CREATE OR REPLACE FUNCTION substrate.radiate(p_blob_unid uuid DEFAULT NULL::uuid, p_repo_path text DEFAULT '/tmp/substrate-events'::text, p_peer_id text DEFAULT 'mythserv1-radiant'::text)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json, subprocess, os
from datetime import datetime

results = {
    'peer': p_peer_id,
    'repo': p_repo_path,
    'forces_written': 0,
    'manifest_updated': False,
    'events_appended': 0,
    'committed': False,
    'pushed': False,
    'commit_hash': None,
    'errors': []
}

def git(args, cwd=p_repo_path):
    r = subprocess.run(['git'] + args, cwd=cwd, capture_output=True, text=True)
    if r.returncode != 0:
        results['errors'].append(f'git {" ".join(args)}: {r.stderr.strip()}')
        return None
    return r.stdout.strip()

# Pull first to avoid conflicts
git(['pull', '--rebase', 'origin', 'main'])

date_str = datetime.utcnow().strftime('%Y-%m-%d')
events_file = os.path.join(p_repo_path, 'events', f'{date_str}.jsonl')
os.makedirs(os.path.dirname(events_file), exist_ok=True)

# If a specific blob was given, radiate just that one
# If NULL, radiate all blobs (full manifest refresh)
if p_blob_unid:
    blob_query = plpy.prepare("""
        SELECT unid, encode(content_hash, 'hex') as content_hash, ordinal, fields, subscriber
        FROM substrate.blob WHERE unid = $1
    """, ["uuid"])
    rows = plpy.execute(blob_query, [p_blob_unid])
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
        # Force blobs: write their SQL content to forces/
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

# Git: add, commit, push
git(['add', '.'])

# Check if there's anything to commit
status = git(['status', '--porcelain'])
if not status:
    results['committed'] = False
    results['pushed'] = False
    return json.dumps(results)

# Build commit message
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

push_out = git(['push', 'origin', 'main'])
if push_out is not None or (not results['errors'] or 'push' not in str(results['errors'][-1:])):
    results['pushed'] = True
    # Push might return empty stdout on success; check if the last error was a push error
    if results['errors'] and 'push' in results['errors'][-1]:
        results['pushed'] = False

# Signal the radiation
if p_blob_unid:
    sig = plpy.prepare("""
        INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
        VALUES ($1, 'radiate', $2::jsonb,
                '00000000-0000-0000-0000-000000000001')
    """, ["uuid", "text"])
    plpy.execute(sig, [p_blob_unid, json.dumps(results)])

return json.dumps(results)
$function$
