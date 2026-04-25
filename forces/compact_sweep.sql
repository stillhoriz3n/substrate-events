CREATE OR REPLACE FUNCTION substrate.compact_sweep(p_min_bytes integer DEFAULT 1048576, p_dry_run boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json

rows = plpy.execute(plpy.prepare("""
    SELECT unid,
           fields->'name'->>'value' as name,
           fields->'composition'->>'value' as composition,
           pg_column_size(fields) as field_bytes,
           COALESCE(fields->'content'->>'type', 'none') as content_type
    FROM substrate.blob
    WHERE pg_column_size(fields) > $1
    AND (fields->'content'->>'type' IS NULL OR fields->'content'->>'type' != 'compressed')
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
    ORDER BY pg_column_size(fields) DESC
""", ["int"]), [p_min_bytes])

results = {
    'min_bytes': p_min_bytes,
    'dry_run': p_dry_run,
    'candidates': len(rows),
    'bytes_before': 0,
    'bytes_after': 0,
    'compressed': []
}

for row in rows:
    entry = {
        'unid': str(row['unid']),
        'name': row['name'],
        'composition': row['composition'],
        'bytes': row['field_bytes'],
        'content_type': row['content_type']
    }
    results['bytes_before'] += row['field_bytes']

    if not p_dry_run:
        try:
            comp_row = plpy.execute(plpy.prepare(
                "SELECT substrate.compress($1, 'zlib') as new_unid", ["uuid"]
            ), [row['unid']])

            new_size = plpy.execute(plpy.prepare(
                "SELECT pg_column_size(fields) as s FROM substrate.blob WHERE unid = $1",
                ["uuid"]
            ), [row['unid']])

            entry['bytes_after'] = new_size[0]['s']
            entry['status'] = 'compressed'
            results['bytes_after'] += new_size[0]['s']
        except Exception as e:
            entry['status'] = f'error: {str(e)[:100]}'
            results['bytes_after'] += row['field_bytes']
    else:
        entry['status'] = 'would_compress'
        results['bytes_after'] += row['field_bytes']

    results['compressed'].append(entry)

results['savings'] = results['bytes_before'] - results['bytes_after']

return json.dumps(results)
$function$
