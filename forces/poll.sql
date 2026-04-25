-- substrate.poll_events — pull new events from the global hash event server
-- Each Babel runs this periodically. Events carry subscriber lists.
-- If my principal is in the list, I fetch the blob from the origin peer.
CREATE OR REPLACE FUNCTION substrate.poll_events(
    p_my_principal TEXT DEFAULT 'SYSTEM',
    p_repo TEXT DEFAULT 'stillhoriz3n/substrate-events'
) RETURNS INT AS $$
import json, subprocess, os, base64
from datetime import datetime

token_row = plpy.execute("SELECT current_setting('substrate.github_token', true) as token")
token = token_row[0]['token'] if token_row and token_row[0]['token'] else os.environ.get('GITHUB_TOKEN', '')

date_str = datetime.utcnow().strftime('%Y-%m-%d')
events_file = f'events/{date_str}.jsonl'

if not token:
    # Try local file
    local_path = f'/tmp/substrate-events/{events_file}'
    if not os.path.exists(local_path):
        return 0
    with open(local_path) as f:
        lines = f.readlines()
else:
    get_cmd = [
        'curl', '-sS',
        '-H', f'Authorization: token {token}',
        '-H', 'Accept: application/vnd.github.v3+json',
        f'https://api.github.com/repos/{p_repo}/contents/{events_file}'
    ]
    result = subprocess.run(get_cmd, capture_output=True, text=True)
    try:
        resp = json.loads(result.stdout)
        content = base64.b64decode(resp['content']).decode('utf-8')
        lines = content.strip().split('\n')
    except:
        return 0

# Get local cursor (last processed event ordinal)
cursor_row = plpy.execute(plpy.prepare("""
    SELECT fields->'cursor'->>'value' as cursor
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'sync_cursor'
    AND fields->'peer'->>'value' = $1
    LIMIT 1
""", ["text"]), [p_repo])

last_ordinal = int(cursor_row[0]['cursor']) if cursor_row else 0

new_events = 0
for line in lines:
    line = line.strip()
    if not line:
        continue
    try:
        event = json.loads(line)
    except:
        continue

    ordinal = event.get('ordinal', 0)
    if ordinal <= last_ordinal:
        continue

    subscriber = event.get('subscriber', [])

    # Check if this event is for me
    if p_my_principal not in subscriber and 'SYSTEM' not in subscriber:
        continue

    # Check if I already have this blob at this hash
    existing = plpy.execute(plpy.prepare(
        "SELECT content_hash FROM substrate.blob WHERE unid = $1::uuid",
        ["text"]
    ), [event['blob_unid']])

    if existing and existing[0]['content_hash'] == event.get('content_hash'):
        continue

    # I need this blob — log it for fetching
    plpy.notice(f'New event: {event["signal_type"]} {event.get("composition","")}/{event.get("name","")} from {event["origin_peer"]} (ordinal {ordinal})')
    new_events += 1

    # Signal that we need to fetch
    sig = plpy.prepare("""
        INSERT INTO substrate.signal (
            blob_unid, signal_type, detail, actor
        ) VALUES (
            $1::uuid, 'sync_needed',
            jsonb_build_object(
                'origin_peer', $2,
                'content_hash', $3,
                'ordinal', $4
            ),
            '00000000-0000-0000-0000-000000000001'
        )
    """, ["text", "text", "text", "int"])
    plpy.execute(sig, [event['blob_unid'], event['origin_peer'], event.get('content_hash', ''), ordinal])

if new_events > 0:
    plpy.notice(f'Poll complete: {new_events} new events for {p_my_principal}')

return new_events
$$ LANGUAGE plpython3u;

-- substrate.sync_from_manifest — bootstrap: pull all blobs I'm subscribed to
-- A fresh machine runs genesis, then calls this to populate from the mesh.
CREATE OR REPLACE FUNCTION substrate.sync_from_manifest(
    p_my_principal TEXT DEFAULT 'SYSTEM',
    p_peer_id TEXT DEFAULT NULL,
    p_repo TEXT DEFAULT 'stillhoriz3n/substrate-events'
) RETURNS INT AS $$
import json, subprocess, os, base64

token_row = plpy.execute("SELECT current_setting('substrate.github_token', true) as token")
token = token_row[0]['token'] if token_row and token_row[0]['token'] else os.environ.get('GITHUB_TOKEN', '')

# List manifest files
if not token:
    manifest_dir = '/tmp/substrate-events/manifest/'
    if not os.path.exists(manifest_dir):
        return 0
    files = [f for f in os.listdir(manifest_dir) if f.endswith('.json')]
    manifests = {}
    for f in files:
        peer = f.replace('.json', '')
        if p_peer_id and peer != p_peer_id:
            continue
        with open(os.path.join(manifest_dir, f)) as fh:
            manifests[peer] = json.load(fh)
else:
    get_cmd = [
        'curl', '-sS',
        '-H', f'Authorization: token {token}',
        '-H', 'Accept: application/vnd.github.v3+json',
        f'https://api.github.com/repos/{p_repo}/contents/manifest'
    ]
    result = subprocess.run(get_cmd, capture_output=True, text=True)
    try:
        files = json.loads(result.stdout)
    except:
        return 0

    manifests = {}
    for f in files:
        if not f['name'].endswith('.json'):
            continue
        peer = f['name'].replace('.json', '')
        if p_peer_id and peer != p_peer_id:
            continue
        get_file = [
            'curl', '-sS',
            '-H', f'Authorization: token {token}',
            '-H', 'Accept: application/vnd.github.v3+json',
            f'https://api.github.com/repos/{p_repo}/contents/manifest/{f["name"]}'
        ]
        file_result = subprocess.run(get_file, capture_output=True, text=True)
        try:
            resp = json.loads(file_result.stdout)
            content = base64.b64decode(resp['content']).decode('utf-8')
            manifests[peer] = json.loads(content)
        except:
            continue

synced = 0
for peer, entries in manifests.items():
    for entry in entries:
        subscriber = entry.get('subscriber', [])
        if p_my_principal not in subscriber and 'SYSTEM' not in subscriber:
            continue

        # Check if I already have this blob
        existing = plpy.execute(plpy.prepare(
            "SELECT content_hash FROM substrate.blob WHERE unid = $1::uuid",
            ["text"]
        ), [entry['unid']])

        if existing and existing[0]['content_hash'] == entry.get('content_hash'):
            continue

        plpy.notice(f'Need blob from {peer}: {entry.get("composition","")}/{entry.get("name","")} ({entry["unid"]})')
        synced += 1

plpy.notice(f'Sync scan complete: {synced} blobs needed from {len(manifests)} peers')
return synced
$$ LANGUAGE plpython3u;
