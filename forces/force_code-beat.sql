CREATE OR REPLACE FUNCTION substrate.beat()
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json
from datetime import datetime

# Compute vitals
vitals = plpy.execute("""
    SELECT 
        (SELECT count(*) FROM substrate.blob WHERE retired_at IS NULL) as active_blobs,
        (SELECT count(*) FROM substrate.blob) as total_blobs,
        (SELECT count(*) FROM substrate.signal) as total_signals,
        (SELECT max(ordinal) FROM substrate.blob) as max_ordinal,
        (SELECT pg_database_size(current_database())) as db_size_bytes,
        (SELECT count(*) FROM substrate.blob 
         WHERE fields->'composition'->>'value' = 'force' AND retired_at IS NULL) as forces,
        (SELECT count(*) FROM substrate.blob 
         WHERE fields->'composition'->>'value' = 'thread' AND retired_at IS NULL) as threads,
        (SELECT count(*) FROM substrate.blob 
         WHERE fields->'composition'->>'value' = 'peer' AND retired_at IS NULL) as peers,
        (SELECT count(*) FROM substrate.blob 
         WHERE fields->'parent_unid' IS NOT NULL AND retired_at IS NULL) as lineaged
""")[0]

# Update or create our heartbeat blob
existing = plpy.execute("""
    SELECT unid FROM substrate.blob 
    WHERE fields->'composition'->>'value' = 'heartbeat'
    AND fields->'peer_id'->>'value' = 'joeys-mac'
    AND retired_at IS NULL
    LIMIT 1
""")

now = datetime.utcnow().isoformat() + 'Z'

heartbeat_fields = {
    'name': {'type': 'utf8', 'value': 'joeys-mac-heartbeat'},
    'composition': {'type': 'utf8', 'value': 'heartbeat'},
    'peer_id': {'type': 'utf8', 'value': 'joeys-mac'},
    'platform': {'type': 'utf8', 'value': 'darwin'},
    'status': {'type': 'utf8', 'value': 'alive'},
    'active_blobs': {'type': 'integer', 'value': str(vitals['active_blobs'])},
    'total_blobs': {'type': 'integer', 'value': str(vitals['total_blobs'])},
    'total_signals': {'type': 'integer', 'value': str(vitals['total_signals'])},
    'max_ordinal': {'type': 'integer', 'value': str(vitals['max_ordinal'])},
    'db_size_bytes': {'type': 'integer', 'value': str(vitals['db_size_bytes'])},
    'forces': {'type': 'integer', 'value': str(vitals['forces'])},
    'threads': {'type': 'integer', 'value': str(vitals['threads'])},
    'peers': {'type': 'integer', 'value': str(vitals['peers'])},
    'lineaged_blobs': {'type': 'integer', 'value': str(vitals['lineaged'])},
    'last_heartbeat': {'type': 'timestamp', 'value': now}
}

if existing:
    plpy.execute(plpy.prepare(
        "UPDATE substrate.blob SET fields = $1::jsonb WHERE unid = $2",
        ["text", "uuid"]
    ), [json.dumps(heartbeat_fields), existing[0]['unid']])
    action = 'updated'
    unid = existing[0]['unid']
else:
    ins = plpy.execute(plpy.prepare(
        "INSERT INTO substrate.blob (fields) VALUES ($1::jsonb) RETURNING unid",
        ["text"]
    ), [json.dumps(heartbeat_fields)])
    action = 'created'
    unid = ins[0]['unid']

return json.dumps({
    'force': 'beat',
    'action': action,
    'unid': str(unid),
    'vitals': dict(vitals)
})
$function$
