CREATE OR REPLACE FUNCTION substrate.circuit_breaker(p_pipe_state_unid uuid, p_max_failures integer DEFAULT 5, p_cooldown_seconds integer DEFAULT 300)
 RETURNS text
 LANGUAGE plpython3u
AS $function$
import json
from datetime import datetime, timedelta

row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_pipe_state_unid])

if not row:
    return 'no_state'

fields = row[0]['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

failures = int(fields.get('failures', {}).get('value', 0))
status = fields.get('status', {}).get('value', 'open')

if status == 'tripped':
    # Check if cooldown has elapsed
    last_emit_str = fields.get('last_emit', {}).get('value', '')
    if last_emit_str:
        try:
            last_emit = datetime.fromisoformat(last_emit_str.replace('+00:00', '').replace('Z', ''))
            elapsed = (datetime.utcnow() - last_emit).total_seconds()
            if elapsed >= p_cooldown_seconds:
                # Reset to half-open
                plpy.execute(plpy.prepare("""
                    UPDATE substrate.blob SET fields = fields
                        || jsonb_build_object(
                            'status', jsonb_build_object('type', 'utf8', 'value', 'half_open'),
                            'failures', jsonb_build_object('type', 'integer', 'value', 0)
                        )
                    WHERE unid = $1
                """, ["uuid"]), [p_pipe_state_unid])
                return 'half_open'
        except:
            pass
    return 'tripped'

if failures >= p_max_failures:
    # Trip the breaker
    plpy.execute(plpy.prepare("""
        UPDATE substrate.blob SET fields = fields
            || jsonb_build_object(
                'status', jsonb_build_object('type', 'utf8', 'value', 'tripped'),
                'tripped_at', jsonb_build_object('type', 'timestamp', 'value', $1)
            )
        WHERE unid = $2
    """, ["text", "uuid"]), [datetime.utcnow().isoformat() + 'Z', p_pipe_state_unid])

    plpy.notice(f'Circuit breaker TRIPPED after {failures} failures')
    return 'tripped'

return status
$function$
