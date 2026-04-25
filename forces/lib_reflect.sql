-- ============================================================
-- SUBSTRATE LIBRARY: lib.reflect
-- The substrate looks at itself. Introspection, health,
-- drift detection, capability inventory.
-- ============================================================

-- ===== FUNCTION CATALOG =====

-- List all substrate functions with argument counts
CREATE OR REPLACE FUNCTION substrate.fn_catalog()
RETURNS TABLE(fn_name TEXT, n_args INT, return_type TEXT, lang TEXT) AS $$
SELECT
    r.routine_name,
    (SELECT COUNT(*)::int FROM information_schema.parameters p
     WHERE p.specific_schema = r.specific_schema
       AND p.specific_name = r.specific_name
       AND p.parameter_mode = 'IN'),
    r.data_type,
    r.external_language
FROM information_schema.routines r
WHERE r.routine_schema = 'substrate'
ORDER BY r.routine_name
$$ LANGUAGE sql;

-- Count functions by language
CREATE OR REPLACE FUNCTION substrate.fn_lang_breakdown()
RETURNS TABLE(lang TEXT, fn_count BIGINT) AS $$
SELECT external_language, COUNT(*)
FROM information_schema.routines
WHERE routine_schema = 'substrate'
GROUP BY external_language
ORDER BY COUNT(*) DESC
$$ LANGUAGE sql;

-- Count functions by prefix (library group)
CREATE OR REPLACE FUNCTION substrate.fn_by_group()
RETURNS JSONB AS $$
import json
result = plpy.execute("""
    SELECT
        CASE
            WHEN routine_name LIKE 'lib_%' THEN split_part(routine_name, '_', 2)
            ELSE split_part(routine_name, '_', 1)
        END AS grp,
        COUNT(*) as cnt
    FROM information_schema.routines
    WHERE routine_schema = 'substrate'
    GROUP BY grp
    HAVING COUNT(*) >= 3
    ORDER BY COUNT(*) DESC
""")
return json.dumps({r['grp']: int(r['cnt']) for r in result})
$$ LANGUAGE plpython3u;

-- Search functions by name pattern
CREATE OR REPLACE FUNCTION substrate.fn_search(pattern TEXT)
RETURNS TABLE(fn_name TEXT, n_args INT, lang TEXT) AS $$
SELECT
    r.routine_name,
    (SELECT COUNT(*)::int FROM information_schema.parameters p
     WHERE p.specific_schema = r.specific_schema
       AND p.specific_name = r.specific_name
       AND p.parameter_mode = 'IN'),
    r.external_language
FROM information_schema.routines r
WHERE r.routine_schema = 'substrate'
  AND r.routine_name ILIKE '%' || pattern || '%'
ORDER BY r.routine_name
$$ LANGUAGE sql;

-- ===== SUBSTRATE HEALTH =====

-- Blob inventory summary
CREATE OR REPLACE FUNCTION substrate.blob_inventory()
RETURNS JSONB AS $$
import json
result = plpy.execute("""
    SELECT
        fields->'composition'->>'value' as composition,
        COUNT(*) as cnt,
        COUNT(*) FILTER (WHERE retired_at IS NULL) as active_cnt,
        MIN(enrolled_at) as oldest,
        MAX(enrolled_at) as newest
    FROM substrate.blob
    GROUP BY fields->'composition'->>'value'
    ORDER BY cnt DESC
""")
inventory = {}
total = 0
for r in result:
    inventory[r['composition'] or 'null'] = {
        'count': int(r['cnt']),
        'active': int(r['active_cnt']),
        'oldest': str(r['oldest']),
        'newest': str(r['newest'])
    }
    total += int(r['cnt'])
return json.dumps({'total_blobs': total, 'by_composition': inventory})
$$ LANGUAGE plpython3u;

-- Signal rate: signals per minute over recent window
CREATE OR REPLACE FUNCTION substrate.signal_rate(window_minutes INT DEFAULT 5)
RETURNS JSONB AS $$
import json
result = plpy.execute(f"""
    SELECT
        signal_type,
        COUNT(*) as cnt
    FROM substrate.signal
    WHERE created_at > now() - interval '{window_minutes} minutes'
    GROUP BY signal_type
    ORDER BY cnt DESC
""")
total = sum(int(r['cnt']) for r in result)
by_type = {r['signal_type']: int(r['cnt']) for r in result}
return json.dumps({
    'window_minutes': window_minutes,
    'total_signals': total,
    'signals_per_minute': round(total / max(1, window_minutes), 2),
    'by_type': by_type
})
$$ LANGUAGE plpython3u;

-- Substrate health score: composite metric
CREATE OR REPLACE FUNCTION substrate.health_score()
RETURNS JSONB AS $$
import json
# Function count
fn_count = plpy.execute("SELECT COUNT(*) as c FROM information_schema.routines WHERE routine_schema = 'substrate'")[0]['c']
# Blob count
blob_count = plpy.execute("SELECT COUNT(*) as c FROM substrate.blob")[0]['c']
# Active blobs
active_count = plpy.execute("SELECT COUNT(*) as c FROM substrate.blob WHERE retired_at IS NULL")[0]['c']
# Compositions in use
comp_count = plpy.execute("SELECT COUNT(DISTINCT fields->'composition'->>'value') as c FROM substrate.blob")[0]['c']
# DB size
db_size = plpy.execute("SELECT pg_size_pretty(pg_database_size('mythos_genesis')) as s")[0]['s']

score = 0
if fn_count >= 100: score += 25
elif fn_count >= 50: score += 15
elif fn_count >= 10: score += 5
if blob_count >= 50: score += 25
elif blob_count >= 20: score += 15
if active_count > 0: score += 25
if comp_count >= 5: score += 25
elif comp_count >= 3: score += 15

grade = 'thriving' if score >= 80 else 'healthy' if score >= 60 else 'growing' if score >= 40 else 'nascent'

return json.dumps({
    'score': score,
    'grade': grade,
    'functions': int(fn_count),
    'blobs': int(blob_count),
    'active_blobs': int(active_count),
    'compositions': int(comp_count),
    'db_size': db_size
})
$$ LANGUAGE plpython3u;

-- ===== DRIFT DETECTION =====

-- Compare function sets between two peers (given as arrays of function names)
CREATE OR REPLACE FUNCTION substrate.fn_drift(local_fns TEXT[], remote_fns TEXT[])
RETURNS JSONB AS $$
import json
local = set(local_fns or [])
remote = set(remote_fns or [])
return json.dumps({
    'local_only': sorted(local - remote),
    'remote_only': sorted(remote - local),
    'shared': len(local & remote),
    'drift_pct': round(100 * len((local ^ remote)) / max(1, len(local | remote)), 1),
    'in_sync': local == remote
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Blob hash drift: compare content_hash between local and remote manifest
CREATE OR REPLACE FUNCTION substrate.blob_drift(local_blobs JSONB, remote_blobs JSONB)
RETURNS JSONB AS $$
import json
local = {b['unid']: b.get('content_hash','') for b in json.loads(local_blobs)}
remote = {b['unid']: b.get('content_hash','') for b in json.loads(remote_blobs)}
stale = []
missing_local = []
missing_remote = []
for unid in set(local) | set(remote):
    if unid not in local:
        missing_local.append(unid)
    elif unid not in remote:
        missing_remote.append(unid)
    elif local[unid] != remote[unid]:
        stale.append(unid)
total = len(set(local) | set(remote))
return json.dumps({
    'total_unique': total,
    'in_sync': len(stale) == 0 and len(missing_local) == 0 and len(missing_remote) == 0,
    'stale': stale,
    'missing_local': missing_local,
    'missing_remote': missing_remote,
    'sync_pct': round(100 * (total - len(stale) - len(missing_local) - len(missing_remote)) / max(1, total), 1)
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Schema version fingerprint: hash of all function signatures
CREATE OR REPLACE FUNCTION substrate.schema_fingerprint()
RETURNS TEXT AS $$
import hashlib
result = plpy.execute("""
    SELECT routine_name, data_type, external_language
    FROM information_schema.routines
    WHERE routine_schema = 'substrate'
    ORDER BY routine_name
""")
sig = '|'.join(f"{r['routine_name']}:{r['data_type']}:{r['external_language']}" for r in result)
return hashlib.sha256(sig.encode()).hexdigest()[:16]
$$ LANGUAGE plpython3u;

-- ===== META-REASONING =====

-- What can the substrate do? Capability summary
CREATE OR REPLACE FUNCTION substrate.capabilities()
RETURNS JSONB AS $$
import json
result = plpy.execute("""
    SELECT routine_name FROM information_schema.routines
    WHERE routine_schema = 'substrate' ORDER BY routine_name
""")
fns = [r['routine_name'] for r in result]
# Classify by prefix pattern
categories = {}
for fn in fns:
    parts = fn.split('_')
    cat = parts[0] if len(parts) > 1 else 'core'
    categories.setdefault(cat, []).append(fn)
# Summarize
summary = {cat: len(fns_list) for cat, fns_list in sorted(categories.items(), key=lambda x: -len(x[1]))}
return json.dumps({
    'total_functions': len(fns),
    'categories': summary,
    'can_perceive': any('dft' in f or 'filter' in f or 'detect' in f for f in fns),
    'can_reason': any('bayes' in f or 'granger' in f or 'entropy' in f for f in fns),
    'can_communicate': any('send' in f or 'broadcast' in f or 'handshake' in f for f in fns),
    'can_learn': any('elo' in f or 'ucb' in f or 'thompson' in f or 'mutate' in f for f in fns),
    'can_self_organize': any('gossip' in f or 'vclock' in f or 'quorum' in f for f in fns),
    'can_introspect': any('catalog' in f or 'health' in f or 'reflect' in f or 'fingerprint' in f for f in fns)
})
$$ LANGUAGE plpython3u;

-- The substrate's own description of itself
CREATE OR REPLACE FUNCTION substrate.whoami_deep()
RETURNS JSONB AS $$
import json, time
health = json.loads(plpy.execute("SELECT substrate.health_score() as h")[0]['h'])
caps = json.loads(plpy.execute("SELECT substrate.capabilities() as c")[0]['c'])
fp = plpy.execute("SELECT substrate.schema_fingerprint() as f")[0]['f']
return json.dumps({
    'identity': 'substrate',
    'version': 'genesis-v1',
    'fingerprint': fp,
    'timestamp': time.time(),
    'health': health,
    'capabilities': caps,
    'philosophy': 'Code and data travel the same pipe. The metaphor is the implementation.'
})
$$ LANGUAGE plpython3u;
