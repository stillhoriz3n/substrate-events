-- substrate.publish — push a blob's hash entry to the global events server (GitHub)
-- This is a force: blob changes → hash goes to the manifest → peers get notified
CREATE OR REPLACE FUNCTION substrate.publish(
    p_blob_unid UUID,
    p_peer_id TEXT DEFAULT 'local',
    p_repo TEXT DEFAULT 'stillhoriz3n/substrate-events'
) RETURNS TEXT AS $$
import json, subprocess, os
from datetime import datetime

row = plpy.execute(plpy.prepare(
    "SELECT unid, content_hash, ordinal, fields, subscriber FROM substrate.blob WHERE unid = $1",
    ["uuid"]
), [p_blob_unid])

if not row:
    plpy.error(f'blob {p_blob_unid} not found')

blob = row[0]
fields = blob['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

event = {
    'event_id': str(p_blob_unid),
    'blob_unid': str(blob['unid']),
    'content_hash': blob['content_hash'],
    'ordinal': blob['ordinal'],
    'composition': fields.get('composition', {}).get('value', ''),
    'name': fields.get('name', {}).get('value', ''),
    'subscriber': list(blob['subscriber']) if blob['subscriber'] else ['SYSTEM'],
    'origin_peer': p_peer_id,
    'signal_type': 'publish',
    'timestamp': datetime.utcnow().isoformat() + 'Z'
}

event_line = json.dumps(event)
date_str = datetime.utcnow().strftime('%Y-%m-%d')
events_file = f'events/{date_str}.jsonl'

# Use GitHub API to append to the events file
# This is the force: one curl, one event, globally visible
token_row = plpy.execute("SELECT current_setting('substrate.github_token', true) as token")
token = token_row[0]['token'] if token_row and token_row[0]['token'] else os.environ.get('GITHUB_TOKEN', '')

if not token:
    # Fallback: write to local file for manual push
    local_path = f'/tmp/substrate-events/{events_file}'
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'a') as f:
        f.write(event_line + '\n')

    result = f'event written locally: {local_path} (no GITHUB_TOKEN set — push manually)'
else:
    import base64

    # Get current file content (if exists)
    get_cmd = [
        'curl', '-sS',
        '-H', f'Authorization: token {token}',
        '-H', 'Accept: application/vnd.github.v3+json',
        f'https://api.github.com/repos/{p_repo}/contents/{events_file}'
    ]
    get_result = subprocess.run(get_cmd, capture_output=True, text=True)

    existing_content = ''
    sha = None
    if get_result.returncode == 0:
        try:
            resp = json.loads(get_result.stdout)
            if 'content' in resp:
                existing_content = base64.b64decode(resp['content']).decode('utf-8')
                sha = resp.get('sha')
        except:
            pass

    new_content = existing_content + event_line + '\n'
    encoded = base64.b64encode(new_content.encode('utf-8')).decode('ascii')

    put_data = {
        'message': f'event: {event["signal_type"]} {event["composition"]}/{event["name"]}',
        'content': encoded
    }
    if sha:
        put_data['sha'] = sha

    put_cmd = [
        'curl', '-sS', '-X', 'PUT',
        '-H', f'Authorization: token {token}',
        '-H', 'Accept: application/vnd.github.v3+json',
        f'https://api.github.com/repos/{p_repo}/contents/{events_file}',
        '-d', json.dumps(put_data)
    ]
    put_result = subprocess.run(put_cmd, capture_output=True, text=True)
    result = f'published to GitHub: {events_file}'

# Signal
sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'publish', jsonb_build_object('peer', $2, 'repo', $3),
            '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text", "text"])
plpy.execute(sig, [p_blob_unid, p_peer_id, p_repo])

return result
$$ LANGUAGE plpython3u;

-- substrate.publish_manifest — push the full peer manifest to GitHub
CREATE OR REPLACE FUNCTION substrate.publish_manifest(
    p_peer_id TEXT DEFAULT 'local',
    p_repo TEXT DEFAULT 'stillhoriz3n/substrate-events'
) RETURNS TEXT AS $$
import json, subprocess, os, base64
from datetime import datetime

rows = plpy.execute("""
    SELECT unid, content_hash, ordinal, fields, subscriber
    FROM substrate.blob
    ORDER BY ordinal
""")

manifest = []
for row in rows:
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
        'origin_endpoint': '',
        'updated_at': datetime.utcnow().isoformat() + 'Z'
    }

    # Include size info for file/package blobs
    if fields.get('size'):
        entry['size'] = int(fields['size'].get('value', 0))
    if fields.get('original_size'):
        entry['original_size'] = int(fields['original_size'].get('value', 0))
    if fields.get('compressed_size'):
        entry['compressed_size'] = int(fields['compressed_size'].get('value', 0))
    if fields.get('content', {}).get('type'):
        entry['content_type'] = fields['content']['type']

    manifest.append(entry)

manifest_json = json.dumps(manifest, indent=2)
manifest_file = f'manifest/{p_peer_id}.json'

token_row = plpy.execute("SELECT current_setting('substrate.github_token', true) as token")
token = token_row[0]['token'] if token_row and token_row[0]['token'] else os.environ.get('GITHUB_TOKEN', '')

if not token:
    local_path = f'/tmp/substrate-events/{manifest_file}'
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'w') as f:
        f.write(manifest_json)
    return f'manifest written locally: {local_path} ({len(manifest)} blobs)'
else:
    encoded = base64.b64encode(manifest_json.encode('utf-8')).decode('ascii')

    # Check if file exists (need SHA for update)
    get_cmd = [
        'curl', '-sS',
        '-H', f'Authorization: token {token}',
        '-H', 'Accept: application/vnd.github.v3+json',
        f'https://api.github.com/repos/{p_repo}/contents/{manifest_file}'
    ]
    get_result = subprocess.run(get_cmd, capture_output=True, text=True)

    sha = None
    try:
        resp = json.loads(get_result.stdout)
        sha = resp.get('sha')
    except:
        pass

    put_data = {
        'message': f'manifest: {p_peer_id} ({len(manifest)} blobs)',
        'content': encoded
    }
    if sha:
        put_data['sha'] = sha

    put_cmd = [
        'curl', '-sS', '-X', 'PUT',
        '-H', f'Authorization: token {token}',
        '-H', 'Accept: application/vnd.github.v3+json',
        f'https://api.github.com/repos/{p_repo}/contents/{manifest_file}',
        '-d', json.dumps(put_data)
    ]
    put_result = subprocess.run(put_cmd, capture_output=True, text=True)

    return f'manifest published to GitHub: {manifest_file} ({len(manifest)} blobs)'

$$ LANGUAGE plpython3u;
