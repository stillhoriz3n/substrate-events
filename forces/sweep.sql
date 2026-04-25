CREATE OR REPLACE FUNCTION substrate.sweep(p_dry_run boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json

results = {
    'dry_run': p_dry_run,
    'anomalies': [],
    'actions': [],
    'summary': {}
}

# 1. EXPIRE — stale messages and orphaned pipe_states
expire_row = plpy.execute(plpy.prepare(
    "SELECT substrate.expire('7 days', '30 days', $1) as result",
    ["bool"]
), [p_dry_run])
expire = json.loads(expire_row[0]['result'])
results['summary']['expired_messages'] = expire['expired_messages']
results['summary']['expired_pipe_states'] = expire['expired_pipe_states']
if expire['expired_messages'] > 0 or expire['expired_pipe_states'] > 0:
    results['actions'].append(expire)

# 2. DUPLICATE COMPOSITIONS — constrain check
dupes = plpy.execute("""
    SELECT fields->'composition'->>'value' as comp,
           fields->'name'->>'value' as name,
           count(*) as ct
    FROM substrate.blob
    WHERE fields->'composition'->>'value' IN ('composition', 'field_type', 'field-type', 'principal', 'package')
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
    GROUP BY 1, 2
    HAVING count(*) > 1
""")
dupe_list = []
for d in dupes:
    dupe_list.append({
        'composition': d['comp'],
        'name': d['name'],
        'count': d['ct']
    })
results['summary']['duplicate_definitions'] = len(dupe_list)
if dupe_list:
    results['anomalies'].append({
        'type': 'duplicate_definitions',
        'details': dupe_list
    })

# 3. NAMING INCONSISTENCY — field-type vs field_type
ft_dash = plpy.execute("""
    SELECT count(*) as ct FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'field-type'
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
""")
ft_under = plpy.execute("""
    SELECT count(*) as ct FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'field_type'
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
""")
dash_ct = ft_dash[0]['ct']
under_ct = ft_under[0]['ct']
if dash_ct > 0 and under_ct > 0:
    results['anomalies'].append({
        'type': 'naming_inconsistency',
        'field': 'composition',
        'variants': {
            'field-type': dash_ct,
            'field_type': under_ct
        },
        'recommendation': f'migrate field-type → field_type ({dash_ct} blobs)'
    })
results['summary']['naming_inconsistencies'] = 1 if (dash_ct > 0 and under_ct > 0) else 0

# 4. COMPACT_SWEEP — large uncompressed blobs
compact_row = plpy.execute(plpy.prepare(
    "SELECT substrate.compact_sweep(1048576, $1) as result",
    ["bool"]
), [p_dry_run])
compact = json.loads(compact_row[0]['result'])
results['summary']['compressible_blobs'] = compact['candidates']
results['summary']['compressible_bytes'] = compact['bytes_before']
if compact['candidates'] > 0:
    results['actions'].append(compact)

# 5. ORPHAN CHECK — blobs referencing retired/missing parents
orphans = plpy.execute("""
    SELECT unid,
           fields->'composition'->>'value' as composition,
           fields->'name'->>'value' as name
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'pipe_state'
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
    AND NOT EXISTS (
        SELECT 1 FROM substrate.blob sub
        WHERE sub.unid::text = (SELECT fields->'subscription'->>'value'
                                FROM substrate.blob ps WHERE ps.unid = substrate.blob.unid)
        AND (sub.fields->'state' IS NULL OR sub.fields->'state'->>'value' NOT IN ('retired', 'reaped'))
    )
""")
orphan_list = []
for o in orphans:
    orphan_list.append({
        'unid': str(o['unid']),
        'composition': o['composition'],
        'name': o['name']
    })
results['summary']['orphans'] = len(orphan_list)
if orphan_list:
    results['anomalies'].append({
        'type': 'orphaned_references',
        'details': orphan_list
    })

# 6. CENSUS
census = plpy.execute("SELECT * FROM substrate.census()")
for row in census:
    results['summary'][row['metric']] = row['value']

# 7. STORAGE
storage = plpy.execute("""
    SELECT pg_database_size(current_database()) as db_bytes,
           pg_total_relation_size('substrate.blob') as blob_bytes,
           pg_total_relation_size('substrate.signal') as signal_bytes
""")
results['summary']['db_size'] = storage[0]['db_bytes']
results['summary']['blob_size'] = storage[0]['blob_bytes']
results['summary']['signal_size'] = storage[0]['signal_bytes']

# Signal
sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ('00000000-0000-0000-0000-000000000001', 'sweep', $1::jsonb,
            '00000000-0000-0000-0000-000000000001')
""", ["text"])
plpy.execute(sig, [json.dumps({'dry_run': p_dry_run, 'summary': results['summary']})])

return json.dumps(results)
$function$
