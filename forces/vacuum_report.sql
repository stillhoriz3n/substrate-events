CREATE OR REPLACE FUNCTION substrate.vacuum_report()
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
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
$function$
