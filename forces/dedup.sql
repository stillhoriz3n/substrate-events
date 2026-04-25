CREATE OR REPLACE FUNCTION substrate.dedup(p_pipe_state_unid uuid, p_content_hash text, p_dedup_window text)
 RETURNS boolean
 LANGUAGE plpython3u
AS $function$
import json
from datetime import datetime, timedelta

if not p_dedup_window or not p_content_hash:
    return True

# Parse ISO 8601 duration (simple: PT1H, PT30M, P1D)
window_seconds = 3600  # default 1 hour
if 'H' in p_dedup_window:
    import re
    m = re.search(r'(\d+)H', p_dedup_window)
    if m:
        window_seconds = int(m.group(1)) * 3600
elif 'M' in p_dedup_window:
    import re
    m = re.search(r'(\d+)M', p_dedup_window)
    if m:
        window_seconds = int(m.group(1)) * 60
elif 'D' in p_dedup_window:
    import re
    m = re.search(r'(\d+)D', p_dedup_window)
    if m:
        window_seconds = int(m.group(1)) * 86400

row = plpy.execute(plpy.prepare(
    "SELECT fields->'dedup_hashes'->>'value' as hashes FROM substrate.blob WHERE unid = $1",
    ["uuid"]
), [p_pipe_state_unid])

now = datetime.utcnow()
cache = {}
if row and row[0]['hashes']:
    try:
        cache = json.loads(row[0]['hashes'])
    except:
        cache = {}

# Evict expired entries
cutoff = (now - timedelta(seconds=window_seconds)).isoformat()
cache = {h: ts for h, ts in cache.items() if ts > cutoff}

# Check for duplicate
if p_content_hash in cache:
    return False

# Record this hash
cache[p_content_hash] = now.isoformat()

plpy.execute(plpy.prepare("""
    UPDATE substrate.blob SET fields = jsonb_set(
        fields, '{dedup_hashes,value}', to_jsonb($1::text)
    ) WHERE unid = $2
""", ["text", "uuid"]), [json.dumps(cache), p_pipe_state_unid])

return True
$function$
