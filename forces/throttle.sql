CREATE OR REPLACE FUNCTION substrate.throttle(p_pipe_state_unid uuid, p_rate_limit text)
 RETURNS boolean
 LANGUAGE plpython3u
AS $function$
import json, re
from datetime import datetime, timedelta

if not p_rate_limit:
    return True

# Parse rate limit: "10/minute", "100/hour", "1000/day"
match = re.match(r'(\d+)/(second|minute|hour|day)', p_rate_limit)
if not match:
    return True

max_count = int(match.group(1))
window = match.group(2)
window_seconds = {'second': 1, 'minute': 60, 'hour': 3600, 'day': 86400}[window]

row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_pipe_state_unid])

if not row:
    return True

fields = row[0]['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

emit_count = int(fields.get('emit_count', {}).get('value', 0))
window_start_str = fields.get('window_start', {}).get('value', '')

now = datetime.utcnow()

# Check if window has elapsed
try:
    window_start = datetime.fromisoformat(window_start_str.replace('+00:00', '').replace('Z', ''))
    elapsed = (now - window_start).total_seconds()
except:
    elapsed = window_seconds + 1

if elapsed >= window_seconds:
    # Reset window
    plpy.execute(plpy.prepare("""
        UPDATE substrate.blob SET fields = fields
            || jsonb_build_object(
                'emit_count', jsonb_build_object('type', 'integer', 'value', 1),
                'window_start', jsonb_build_object('type', 'timestamp', 'value', $1)
            )
        WHERE unid = $2
    """, ["text", "uuid"]), [now.isoformat() + 'Z', p_pipe_state_unid])
    return True

if emit_count >= max_count:
    return False

# Increment counter
plpy.execute(plpy.prepare("""
    UPDATE substrate.blob SET fields = jsonb_set(
        fields, '{emit_count,value}', to_jsonb(($1)::int)
    ) WHERE unid = $2
""", ["int", "uuid"]), [emit_count + 1, p_pipe_state_unid])

return True
$function$
