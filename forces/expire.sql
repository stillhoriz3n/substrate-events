CREATE OR REPLACE FUNCTION substrate.expire(p_message_ttl interval DEFAULT '7 days'::interval, p_pipe_state_ttl interval DEFAULT '30 days'::interval, p_dry_run boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json

results = {
    'message_ttl': str(p_message_ttl),
    'pipe_state_ttl': str(p_pipe_state_ttl),
    'dry_run': p_dry_run,
    'expired_messages': 0,
    'expired_pipe_states': 0,
    'expired_emissions': 0,
    'details': []
}

# 1. Stale messages — pending for longer than TTL
stale_msgs = plpy.execute(plpy.prepare("""
    SELECT unid,
           fields->'from'->>'value' as sender,
           fields->'to'->>'value' as recipient,
           fields->'subject'->>'value' as subject,
           fields->'status'->>'value' as status,
           fields->'sent_at'->>'value' as sent_at
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'message'
    AND fields->'status'->>'value' IN ('pending', 'delivered')
    AND fields->'sent_at'->>'value' != ''
    AND (fields->'sent_at'->>'value')::timestamptz < (now() - $1)
""", ["interval"]), [p_message_ttl])

for msg in stale_msgs:
    entry = {
        'type': 'message',
        'unid': str(msg['unid']),
        'from': msg['sender'],
        'to': msg['recipient'],
        'subject': msg['subject'],
        'status': msg['status'],
        'sent_at': msg['sent_at']
    }

    if not p_dry_run:
        plpy.execute(plpy.prepare("""
            UPDATE substrate.blob SET fields = fields || jsonb_build_object(
                'status', jsonb_build_object('type', 'status', 'value', 'expired'),
                'expired_at', jsonb_build_object('type', 'timestamp', 'value', now()::text)
            ) WHERE unid = $1
        """, ["uuid"]), [msg['unid']])
        entry['action'] = 'expired'
    else:
        entry['action'] = 'would_expire'

    results['details'].append(entry)
    results['expired_messages'] += 1

# 2. Orphaned pipe_states — subscription no longer exists
orphan_pipes = plpy.execute("""
    SELECT ps.unid,
           ps.fields->'subscription'->>'value' as sub_ref
    FROM substrate.blob ps
    WHERE ps.fields->'composition'->>'value' = 'pipe_state'
    AND (ps.fields->'state' IS NULL OR ps.fields->'state'->>'value' NOT IN ('retired', 'reaped'))
    AND NOT EXISTS (
        SELECT 1 FROM substrate.blob sub
        WHERE sub.unid::text = ps.fields->'subscription'->>'value'
        AND sub.fields->'composition'->>'value' = 'subscription'
        AND (sub.fields->'state' IS NULL OR sub.fields->'state'->>'value' NOT IN ('retired', 'reaped'))
    )
""")

for pipe in orphan_pipes:
    entry = {
        'type': 'orphaned_pipe_state',
        'unid': str(pipe['unid']),
        'dead_subscription': pipe['sub_ref']
    }

    if not p_dry_run:
        plpy.execute(plpy.prepare(
            "SELECT substrate.retire($1)", ["uuid"]
        ), [pipe['unid']])
        entry['action'] = 'retired'
    else:
        entry['action'] = 'would_retire'

    results['details'].append(entry)
    results['expired_pipe_states'] += 1

# 3. Stale pipe_states — last_emit older than TTL
stale_pipes = plpy.execute(plpy.prepare("""
    SELECT unid,
           fields->'subscription'->>'value' as sub_ref,
           fields->'last_emit'->>'value' as last_emit
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'pipe_state'
    AND fields->'last_emit'->>'value' != ''
    AND (fields->'last_emit'->>'value')::timestamptz < (now() - $1)
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
""", ["interval"]), [p_pipe_state_ttl])

for pipe in stale_pipes:
    entry = {
        'type': 'stale_pipe_state',
        'unid': str(pipe['unid']),
        'subscription': pipe['sub_ref'],
        'last_emit': pipe['last_emit']
    }

    if not p_dry_run:
        plpy.execute(plpy.prepare(
            "SELECT substrate.retire($1)", ["uuid"]
        ), [pipe['unid']])
        entry['action'] = 'retired'
    else:
        entry['action'] = 'would_retire'

    results['details'].append(entry)
    results['expired_pipe_states'] += 1

# Signal
if not p_dry_run:
    sig = plpy.prepare("""
        INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
        VALUES ('00000000-0000-0000-0000-000000000001', 'expire', $1::jsonb,
                '00000000-0000-0000-0000-000000000001')
    """, ["text"])
    plpy.execute(sig, [json.dumps(results)])

return json.dumps(results)
$function$
