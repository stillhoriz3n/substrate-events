CREATE OR REPLACE FUNCTION substrate.reap(p_grace_period interval DEFAULT '7 days'::interval, p_mode text DEFAULT 'purge'::text, p_dry_run boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json

rows = plpy.execute(plpy.prepare("""
    SELECT unid,
           fields->'composition'->>'value' as composition,
           fields->'name'->>'value' as name,
           retired_at,
           pg_column_size(fields) as field_bytes
    FROM substrate.blob
    WHERE retired_at IS NOT NULL
    AND retired_at < (now() - $1::interval)
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('reaped'))
    ORDER BY retired_at
""", ["interval"]), [p_grace_period])

results = {
    'grace_period': str(p_grace_period),
    'mode': p_mode,
    'dry_run': p_dry_run,
    'candidates': len(rows),
    'bytes_reclaimable': 0,
    'reaped': []
}

# Activate the reaper once for the entire transaction
if not p_dry_run and p_mode == 'delete':
    plpy.execute("SET LOCAL substrate.reaper_active = 'true'")

for row in rows:
    entry = {
        'unid': str(row['unid']),
        'composition': row['composition'],
        'name': row['name'],
        'retired_at': str(row['retired_at']),
        'bytes': row['field_bytes']
    }
    results['bytes_reclaimable'] += row['field_bytes']
    results['reaped'].append(entry)

    if not p_dry_run:
        if p_mode == 'purge':
            # Record reap signal first
            sig = plpy.prepare("""
                INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
                VALUES ($1, 'reap', jsonb_build_object('mode', 'purge', 'bytes', $2, 'name', $3),
                        '00000000-0000-0000-0000-000000000001')
            """, ["uuid", "text", "text"])
            plpy.execute(sig, [row['unid'], str(row['field_bytes']), row['name']])

            # Null out content, keep row as tombstone
            plpy.execute(plpy.prepare("""
                UPDATE substrate.blob SET fields = jsonb_build_object(
                    'composition', fields->'composition',
                    'name', fields->'name',
                    'state', jsonb_build_object('type', 'utf8', 'value', 'reaped'),
                    'reaped_at', jsonb_build_object('type', 'timestamp', 'value', now()::text),
                    'original_size', jsonb_build_object('type', 'integer', 'value', $1)
                ) WHERE unid = $2
            """, ["int", "uuid"]), [row['field_bytes'], row['unid']])

        elif p_mode == 'delete':
            # Record reap signal FIRST — point it at REAPED sentinel
            # since the blob row is about to be deleted
            sig = plpy.prepare("""
                INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
                VALUES ('00000000-0000-0000-0000-000000000000', 'reap',
                        jsonb_build_object('mode', 'delete', 'original_unid', $1,
                                           'bytes', $2, 'name', $3),
                        '00000000-0000-0000-0000-000000000001')
            """, ["text", "text", "text"])
            plpy.execute(sig, [str(row['unid']), str(row['field_bytes']), row['name']])

            # Redirect existing signals to REAPED sentinel
            # reaper_active is already SET LOCAL above
            plpy.execute(plpy.prepare("""
                UPDATE substrate.signal
                SET blob_unid = '00000000-0000-0000-0000-000000000000'
                WHERE blob_unid = $1
            """, ["uuid"]), [row['unid']])

            # Delete the blob row
            plpy.execute(plpy.prepare(
                "DELETE FROM substrate.blob WHERE unid = $1", ["uuid"]
            ), [row['unid']])

results['bytes_saved'] = results['bytes_reclaimable']
return json.dumps(results)
$function$
