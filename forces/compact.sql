CREATE OR REPLACE FUNCTION substrate.compact(p_composition text DEFAULT NULL::text, p_dry_run boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json, zlib, base64

# Find blobs that have content and could be compacted
query = """
    SELECT unid,
           fields->'composition'->>'value' as composition,
           fields->'name'->>'value' as name,
           fields->'content'->>'value' as content,
           fields->'content'->>'type' as content_type,
           ordinal,
           pg_column_size(fields) as field_bytes
    FROM substrate.blob
    WHERE fields->'content'->>'value' IS NOT NULL
    AND fields->'content'->>'value' != ''
    AND (fields->'compacted' IS NULL OR fields->'compacted'->>'value' != 'true')
"""
if p_composition:
    query += f" AND fields->'composition'->>'value' = '{p_composition}'"
query += " ORDER BY fields->'name'->>'value', ordinal DESC"

rows = plpy.execute(query)

# Group by name+composition
groups = {}
for row in rows:
    key = f"{row['composition']}:{row['name']}"
    if key not in groups:
        groups[key] = []
    groups[key].append(row)

results = {
    'groups_scanned': len(groups),
    'blobs_scanned': len(rows),
    'candidates': 0,
    'bytes_before': 0,
    'bytes_after': 0,
    'bytes_saved': 0,
    'compacted': [],
    'dry_run': p_dry_run
}

for key, versions in groups.items():
    if len(versions) < 2:
        continue

    # Newest version is the base (kept full). Older versions get delta'd.
    base = versions[0]  # highest ordinal = newest
    base_content = base['content']

    for older in versions[1:]:
        older_content = older['content']
        if not older_content:
            continue

        older_bytes = len(older_content.encode('utf-8'))

        # Compute delta: what do you need to add to base to get older?
        # Using zlib's compress on the XOR/diff isn't ideal for text,
        # so we store a compressed copy of the older content with a
        # reference to the base. Reconstruction = decompress the delta.
        #
        # For binary content (base64), the delta is the compressed
        # difference. For identical content, delta is near-zero.

        # Simple approach: compress the older content referencing base
        older_raw = older_content.encode('utf-8')
        base_raw = base_content.encode('utf-8')

        # If contents are identical, delta is empty
        if older_raw == base_raw:
            delta = b''
            delta_b64 = ''
        else:
            # Store compressed full content as delta (future: xdelta3)
            delta = zlib.compress(older_raw, 9)
            delta_b64 = base64.b64encode(delta).decode('ascii')

        delta_bytes = len(delta_b64.encode('utf-8')) if delta_b64 else 0
        saved = older_bytes - delta_bytes

        results['candidates'] += 1
        results['bytes_before'] += older_bytes
        results['bytes_after'] += delta_bytes

        entry = {
            'unid': str(older['unid']),
            'name': older['name'],
            'original_bytes': older_bytes,
            'delta_bytes': delta_bytes,
            'saved': saved,
            'base_unid': str(base['unid'])
        }
        results['compacted'].append(entry)

        if not p_dry_run and saved > 0:
            # Replace content with delta, mark as compacted
            plan = plpy.prepare("""
                UPDATE substrate.blob SET fields = fields
                    || jsonb_build_object(
                        'content', jsonb_build_object(
                            'type', 'delta',
                            'value', $1
                        ),
                        'compacted', jsonb_build_object('type', 'boolean', 'value', 'true'),
                        'base_version', jsonb_build_object('type', 'reference', 'value', $2),
                        'original_size', jsonb_build_object('type', 'integer', 'value', $3)
                    )
                WHERE unid = $4
            """, ["text", "text", "int", "uuid"])
            plpy.execute(plan, [delta_b64, str(base['unid']), older_bytes, older['unid']])

            # Signal
            sig = plpy.prepare("""
                INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
                VALUES ($1, 'compact', jsonb_build_object(
                    'base', $2, 'original_bytes', $3, 'delta_bytes', $4
                ), '00000000-0000-0000-0000-000000000001')
            """, ["uuid", "text", "int", "int"])
            plpy.execute(sig, [older['unid'], str(base['unid']), older_bytes, delta_bytes])

results['bytes_saved'] = results['bytes_before'] - results['bytes_after']

return json.dumps(results)
$function$
