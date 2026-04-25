CREATE OR REPLACE FUNCTION substrate.migrate(p_field text, p_old_value text, p_new_value text, p_dry_run boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json

rows = plpy.execute(plpy.prepare("""
    SELECT unid,
           fields->'name'->>'value' as name,
           fields->'composition'->>'value' as composition
    FROM substrate.blob
    WHERE fields->$1->>'value' = $2
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
""", ["text", "text"]), [p_field, p_old_value])

results = {
    'field': p_field,
    'old_value': p_old_value,
    'new_value': p_new_value,
    'dry_run': p_dry_run,
    'candidates': len(rows),
    'migrated': []
}

for row in rows:
    entry = {
        'unid': str(row['unid']),
        'name': row['name'],
        'composition': row['composition']
    }

    if not p_dry_run:
        plpy.execute(plpy.prepare(f"""
            UPDATE substrate.blob
            SET fields = jsonb_set(fields, '{{{p_field},value}}', to_jsonb($1::text))
            WHERE unid = $2
        """, ["text", "uuid"]), [p_new_value, row['unid']])
        entry['status'] = 'migrated'
    else:
        entry['status'] = 'would_migrate'

    results['migrated'].append(entry)

if not p_dry_run and results['candidates'] > 0:
    sig = plpy.prepare("""
        INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
        VALUES ('00000000-0000-0000-0000-000000000001', 'migrate', $1::jsonb,
                '00000000-0000-0000-0000-000000000001')
    """, ["text"])
    plpy.execute(sig, [json.dumps(results)])

return json.dumps(results)
$function$
