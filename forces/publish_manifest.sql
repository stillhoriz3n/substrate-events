CREATE OR REPLACE FUNCTION substrate.publish_manifest(p_peer_id text DEFAULT 'local'::text, p_repo text DEFAULT 'stillhoriz3n/substrate-events'::text)
 RETURNS text
 LANGUAGE plpython3u
AS $function$
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

$function$
