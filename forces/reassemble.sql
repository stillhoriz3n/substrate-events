CREATE OR REPLACE FUNCTION substrate.reassemble(p_package_unid uuid)
 RETURNS text
 LANGUAGE plpython3u
AS $function$
import base64, os, json

# Get package metadata
row = plpy.execute(f"""
    SELECT fields->'path'->>'value' as path,
           fields->'permission'->>'value' as perm,
           fields->'chunks'->'value' as chunks
    FROM substrate.blob WHERE unid = '{p_package_unid}'
""")
if not row:
    return 'package blob not found'

path = row[0]['path']
perm = row[0]['perm']
chunks_raw = row[0]['chunks']

# Parse chunk UUIDs
chunk_ids = json.loads(chunks_raw) if isinstance(chunks_raw, str) else list(chunks_raw)

parent = os.path.dirname(path)
if parent:
    os.makedirs(parent, exist_ok=True)

# Reassemble: read each chunk content, decode, concatenate
total = 0
with open(path, 'wb') as f:
    for i, cid in enumerate(chunk_ids):
        crow = plpy.execute(f"""
            SELECT fields->'content'->>'value' as content
            FROM substrate.blob WHERE unid = '{cid}'
        """)
        if not crow:
            return f'chunk {i} ({cid}) not found'
        data = base64.b64decode(crow[0]['content'])
        f.write(data)
        total += len(data)
        plpy.notice(f'chunk {i+1}/{len(chunk_ids)}: {len(data)} bytes')

if perm:
    try:
        os.chmod(path, int(perm, 8))
    except:
        pass

# Signal
plpy.execute(f"""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ('{p_package_unid}', 'materialize',
            '{{"path": "{path}", "size": {total}, "chunks": {len(chunk_ids)}}}'::jsonb,
            '00000000-0000-0000-0000-000000000001')
""")

return f'reassembled {total} bytes ({len(chunk_ids)} chunks) to {path}'
$function$
