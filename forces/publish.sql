CREATE OR REPLACE FUNCTION substrate.publish(p_blob_unid uuid, p_peer_id text DEFAULT 'local'::text, p_repo text DEFAULT 'stillhoriz3n/substrate-events'::text)
 RETURNS text
 LANGUAGE plpython3u
AS $function$
import json, os
from datetime import datetime

row = plpy.execute(plpy.prepare(
    "SELECT unid, encode(content_hash, 'hex') as content_hash, ordinal, fields, subscriber FROM substrate.blob WHERE unid = $1",
    ["uuid"]
), [p_blob_unid])

if not row:
    plpy.error(f'blob {p_blob_unid} not found')

blob = row[0]
fields = blob['fields']
if isinstance(fields, str):
    import json as j
    fields = j.loads(fields)

event = {
    'event_id': str(p_blob_unid),
    'blob_unid': str(blob['unid']),
    'content_hash': blob['content_hash'] or '',
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

local_path = f'/tmp/substrate-events/{events_file}'
os.makedirs(os.path.dirname(local_path), exist_ok=True)
with open(local_path, 'a') as f:
    f.write(event_line + '\n')

sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'publish', jsonb_build_object('peer', $2, 'repo', $3),
            '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text", "text"])
plpy.execute(sig, [p_blob_unid, p_peer_id, p_repo])

return 'published: ' + event['name']
$function$
