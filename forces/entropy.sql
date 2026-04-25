-- ============================================================
-- ENTROPY FORCES — thermodynamics of storage
--
-- Law 4 says blobs cannot be deleted, only retired.
-- These two forces manage what happens AFTER that:
--
-- 1. compact  — delta-compress blob versions (living blobs)
-- 2. reap     — garbage-collect retired blobs (dead blobs)
--
-- Neither violates Conservation (signals are permanent).
-- Both are recorded in the signal chain before they act.
-- ============================================================

-- ============================================================
-- FORCE: compact — delta-compress blob versions
--
-- When a blob is updated, the old content still exists as a
-- full copy. compact() finds blobs that share a lineage
-- (same name+composition, different ordinals) and replaces
-- older versions' content with a delta from the newest version.
--
-- Reconstruction: base version (newest, full) + delta = old version.
-- Like git packfiles: newest is full, ancestors are deltas.
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.compact(
    p_composition TEXT DEFAULT NULL,
    p_dry_run BOOLEAN DEFAULT true
) RETURNS JSONB AS $$
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
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: reap — garbage-collect retired blobs
--
-- Law 4 (Entropy) prevents DELETE. But retirement is a state,
-- not a promise of eternal storage. reap() finds blobs that
-- have been retired for longer than the grace period, records
-- the reaping in the signal chain, then PURGES the content.
--
-- Two modes:
--   'purge'  — null out the content field, keep the row (default)
--   'delete' — remove the row entirely (aggressive)
--
-- The signal chain is NEVER touched. Conservation holds.
-- The reaping itself is a signal — recorded before it happens.
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.reap(
    p_grace_period INTERVAL DEFAULT '7 days',
    p_mode TEXT DEFAULT 'purge',
    p_dry_run BOOLEAN DEFAULT true
) RETURNS JSONB AS $$
import json

# Find retired blobs past the grace period
rows = plpy.execute(plpy.prepare("""
    SELECT unid,
           fields->'composition'->>'value' as composition,
           fields->'name'->>'value' as name,
           fields->'state'->>'value' as state,
           retired_at,
           pg_column_size(fields) as field_bytes
    FROM substrate.blob
    WHERE retired_at IS NOT NULL
    AND retired_at < (now() - $1::interval)
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
        # Record the reaping BEFORE it happens (Conservation)
        sig = plpy.prepare("""
            INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
            VALUES ($1, 'reap', jsonb_build_object(
                'mode', $2,
                'grace_period', $3,
                'bytes_reclaimed', $4
            ), '00000000-0000-0000-0000-000000000001')
        """, ["uuid", "text", "text", "int"])
        plpy.execute(sig, [row['unid'], p_mode, str(p_grace_period), row['field_bytes']])

        if p_mode == 'purge':
            # Null out the content, keep the row as a tombstone
            plpy.execute(plpy.prepare("""
                UPDATE substrate.blob SET fields = jsonb_build_object(
                    'composition', fields->'composition',
                    'name', fields->'name',
                    'state', jsonb_build_object('type', 'utf8', 'value', 'reaped'),
                    'reaped_at', jsonb_build_object('type', 'timestamp', 'value', now()::text),
                    'original_size', jsonb_build_object('type', 'integer', 'value', $1)
                )
                WHERE unid = $2
            """, ["int", "uuid"]), [row['field_bytes'], row['unid']])

        elif p_mode == 'delete':
            # Full removal — the row is gone, signals persist
            # We need to bypass force_entropy (the DELETE trigger)
            # Temporarily disable the trigger for this operation
            plpy.execute("SET LOCAL substrate.reaper_active = 'true'")
            plpy.execute(plpy.prepare(
                "DELETE FROM substrate.blob WHERE unid = $1", ["uuid"]
            ), [row['unid']])

return json.dumps(results)
$$ LANGUAGE plpython3u;

-- ============================================================
-- Update force_entropy to allow reaper deletes
-- The reaper is the ONLY force that can delete. It must
-- identify itself via the session variable.
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.force_entropy()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if the reaper is active (it's the only force allowed to delete)
    IF current_setting('substrate.reaper_active', true) = 'true' THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'blobs cannot be deleted, only retired';
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- FORCE: retire — the proper way to mark a blob for future reaping
-- Sets retired_at timestamp. The blob stays until reap() runs.
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.retire(p_unid UUID)
RETURNS VOID AS $$

plpy.execute(plpy.prepare("""
    UPDATE substrate.blob SET
        retired_at = now(),
        fields = fields || jsonb_build_object(
            'state', jsonb_build_object('type', 'utf8', 'value', 'retired')
        )
    WHERE unid = $1
    AND retired_at IS NULL
""", ["uuid"]), [p_unid])

sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'retire', '{}', '00000000-0000-0000-0000-000000000001')
""", ["uuid"])
plpy.execute(sig, [p_unid])
$$ LANGUAGE plpython3u;

-- ============================================================
-- FORCE: vacuum_report — show what's consuming space and what
-- can be reclaimed by compact + reap
-- ============================================================
CREATE OR REPLACE FUNCTION substrate.vacuum_report()
RETURNS JSONB AS $$
import json

# Total DB size
db_size = plpy.execute("""
    SELECT pg_database_size('mythos_genesis') as bytes,
           pg_size_pretty(pg_database_size('mythos_genesis')) as pretty
""")[0]

# Blob table size
blob_size = plpy.execute("""
    SELECT pg_total_relation_size('substrate.blob') as bytes,
           pg_size_pretty(pg_total_relation_size('substrate.blob')) as pretty
""")[0]

# Signal table size
sig_size = plpy.execute("""
    SELECT pg_total_relation_size('substrate.signal') as bytes,
           pg_size_pretty(pg_total_relation_size('substrate.signal')) as pretty
""")[0]

# Content breakdown by composition
breakdown = plpy.execute("""
    SELECT
        fields->'composition'->>'value' as composition,
        count(*) as count,
        pg_size_pretty(sum(pg_column_size(fields))::bigint) as total_size,
        sum(pg_column_size(fields)) as total_bytes
    FROM substrate.blob
    GROUP BY fields->'composition'->>'value'
    ORDER BY sum(pg_column_size(fields)) DESC
""")

# Duplicate content (same name, multiple versions)
dupes = plpy.execute("""
    SELECT
        fields->'name'->>'value' as name,
        count(*) as versions,
        pg_size_pretty(sum(pg_column_size(fields))::bigint) as total_size,
        sum(pg_column_size(fields)) as total_bytes
    FROM substrate.blob
    WHERE fields->'content'->>'value' IS NOT NULL
    AND fields->'content'->>'value' != ''
    GROUP BY fields->'name'->>'value'
    HAVING count(*) > 1
    ORDER BY sum(pg_column_size(fields)) DESC
""")

# Retired blobs
retired = plpy.execute("""
    SELECT count(*) as count,
           COALESCE(pg_size_pretty(sum(pg_column_size(fields))::bigint), '0 bytes') as total_size,
           COALESCE(sum(pg_column_size(fields)), 0) as total_bytes
    FROM substrate.blob
    WHERE retired_at IS NOT NULL
""")

# Top 10 biggest blobs
top10 = plpy.execute("""
    SELECT
        fields->'name'->>'value' as name,
        fields->'composition'->>'value' as composition,
        pg_size_pretty(pg_column_size(fields)::bigint) as size,
        pg_column_size(fields) as bytes,
        fields->'content'->>'type' as content_type
    FROM substrate.blob
    ORDER BY pg_column_size(fields) DESC
    LIMIT 10
""")

report = {
    'database': {'size': db_size['pretty'], 'bytes': db_size['bytes']},
    'blob_table': {'size': blob_size['pretty'], 'bytes': blob_size['bytes']},
    'signal_table': {'size': sig_size['pretty'], 'bytes': sig_size['bytes']},
    'by_composition': [
        {'composition': r['composition'], 'count': r['count'], 'size': r['total_size'], 'bytes': r['total_bytes']}
        for r in breakdown
    ],
    'duplicate_versions': [
        {'name': r['name'], 'versions': r['versions'], 'size': r['total_size'], 'bytes': r['total_bytes']}
        for r in dupes
    ],
    'retired': {
        'count': retired[0]['count'],
        'size': retired[0]['total_size'],
        'bytes': retired[0]['total_bytes']
    },
    'top_10': [
        {'name': r['name'], 'composition': r['composition'], 'size': r['size'], 'content_type': r['content_type']}
        for r in top10
    ],
    'recommendations': []
}

# Generate recommendations
if dupes:
    total_dupe_bytes = sum(r['total_bytes'] for r in dupes)
    report['recommendations'].append(
        f'compact: {len(dupes)} blobs have multiple versions using {total_dupe_bytes // 1048576}MB — run substrate.compact(dry_run := false)'
    )

if retired[0]['count'] > 0:
    report['recommendations'].append(
        f'reap: {retired[0]["count"]} retired blobs using {retired[0]["total_size"]} — run substrate.reap(dry_run := false)'
    )

blob_pct = (blob_size['bytes'] / db_size['bytes'] * 100) if db_size['bytes'] > 0 else 0
if blob_pct > 80:
    report['recommendations'].append(
        f'blob table is {blob_pct:.0f}% of database — consider archiving old file blobs'
    )

return json.dumps(report)
$$ LANGUAGE plpython3u;
