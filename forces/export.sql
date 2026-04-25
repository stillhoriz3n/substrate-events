CREATE OR REPLACE FUNCTION substrate.export(p_composition text DEFAULT NULL::text, p_repo_path text DEFAULT '/tmp/substrate-events'::text)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json, os
from datetime import datetime

# Query blobs to export
if p_composition:
    rows = plpy.execute(plpy.prepare("""
        SELECT unid, fields, subscriber, encode(content_hash, 'hex') as content_hash
        FROM substrate.blob
        WHERE fields->'composition'->>'value' = $1
        AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
        ORDER BY ordinal
    """, ['text']), [p_composition])
else:
    # Export publishable blobs only
    rows = plpy.execute("""
        SELECT unid, fields, subscriber, encode(content_hash, 'hex') as content_hash
        FROM substrate.blob
        WHERE fields->'composition'->>'value' NOT IN ('field_type', 'composition', 'memory')
        AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
        AND pg_column_size(fields) < 1048576
        ORDER BY ordinal
    """)

peer_id = plpy.execute("SELECT current_setting('substrate.peer_id', true) as p")[0]['p'] or 'unknown'
export_dir = os.path.join(p_repo_path, 'blobs', peer_id)
os.makedirs(export_dir, exist_ok=True)

exported = 0
for row in rows:
    fields = row['fields']
    if isinstance(fields, str):
        fields = json.loads(fields)

    comp = fields.get('composition', {}).get('value', '')
    name = fields.get('name', {}).get('value', '')
    
    blob_data = {
        'unid': str(row['unid']),
        'fields': fields,
        'subscriber': list(row['subscriber']) if row['subscriber'] else ['SYSTEM'],
        'content_hash': row['content_hash'],
        'origin_peer': peer_id,
        'exported_at': datetime.utcnow().isoformat() + 'Z'
    }
    
    safe_name = name.replace('/', '_').replace(' ', '_')[:60]
    filename = f'{comp}--{safe_name}.json'
    filepath = os.path.join(export_dir, filename)
    
    with open(filepath, 'w') as f:
        f.write(json.dumps(blob_data, indent=2))
    exported += 1

return json.dumps({
    'peer': peer_id,
    'exported': exported,
    'directory': export_dir
})
$function$
