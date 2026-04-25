CREATE OR REPLACE FUNCTION substrate."fetch"(p_peer_name text DEFAULT NULL::text)
 RETURNS integer
 LANGUAGE plpython3u
AS $function$
import json

# Find pending sync entries
pending = plpy.execute("""
    SELECT unid, 
           fields->'content'->>'value' as detail,
           fields->'origin_peer'->>'value' as origin_peer
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'memory'
    AND fields->'sync_status'->>'value' = 'pending'
    ORDER BY ordinal
""")

if not pending:
    plpy.notice('Nothing to fetch')
    return 0

# Group by origin peer
by_peer = {}
for p in pending:
    peer = p['origin_peer']
    if p_peer_name and peer != p_peer_name:
        continue
    if peer not in by_peer:
        by_peer[peer] = []
    by_peer[peer].append(p)

fetched = 0
for peer_name, entries in by_peer.items():
    # Find peer blob for connection info
    peer_row = plpy.execute(plpy.prepare("""
        SELECT fields->'endpoint'->>'value' as endpoint,
               fields->'transport'->>'value' as transport
        FROM substrate.blob
        WHERE fields->'composition'->>'value' = 'peer'
        AND fields->'name'->>'value' = $1
        LIMIT 1
    """, ["text"]), [peer_name])
    
    if not peer_row:
        plpy.notice(f'No peer blob for {peer_name} — cannot fetch')
        continue
    
    endpoint = peer_row[0]['endpoint']
    transport = peer_row[0]['transport']
    
    if transport != 'postgres':
        plpy.notice(f'Peer {peer_name} uses {transport} transport — only postgres supported')
        continue
    
    # Parse endpoint into host:port
    parts = endpoint.split(':')
    host = parts[0]
    port = parts[1] if len(parts) > 1 else '5432'
    
    connstr = f'host={host} port={port} dbname=mythos_genesis user=postgres'
    plpy.notice(f'Connecting to {peer_name} at {connstr}')
    
    for entry in entries:
        detail = json.loads(entry['detail'])
        blob_unid = detail.get('blob_unid')
        blob_name = detail.get('name', '?')
        blob_comp = detail.get('composition', '?')
        
        plpy.notice(f'Fetching {blob_comp}/{blob_name} ({blob_unid})')
        
        try:
            # Use dblink to query the remote blob
            result = plpy.execute(plpy.prepare("""
                SELECT * FROM dblink($1, 
                    'SELECT fields, subscriber FROM substrate.blob WHERE unid = ''' || $2 || '''::uuid'
                ) AS t(fields jsonb, subscriber text[])
            """, ["text", "text"]), [connstr, blob_unid])
            
            if result:
                remote_fields = result[0]['fields']
                remote_sub = result[0]['subscriber']
                
                if isinstance(remote_fields, str):
                    remote_fields = json.loads(remote_fields)
                
                # Insert the blob locally
                plpy.execute(plpy.prepare("""
                    INSERT INTO substrate.blob (unid, fields, subscriber)
                    VALUES ($1::uuid, $2::jsonb, $3)
                """, ["text", "text", "text[]"]), [blob_unid, json.dumps(remote_fields), remote_sub])
                
                # Mark sync entry as complete
                plpy.execute(plpy.prepare("""
                    UPDATE substrate.blob 
                    SET fields = jsonb_set(fields, '{sync_status,value}', '"fetched"'::jsonb)
                    WHERE unid = $1
                """, ["uuid"]), [entry['unid']])
                
                fetched += 1
                plpy.notice(f'Fetched: {blob_comp}/{blob_name}')
            else:
                plpy.notice(f'Not found on remote: {blob_unid}')
        except Exception as e:
            plpy.warning(f'Fetch failed for {blob_name}: {str(e)}')
            continue

plpy.notice(f'Fetch complete: {fetched} blobs pulled')
return fetched
$function$
