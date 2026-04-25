-- ============================================================
-- SUBSTRATE LIBRARY: lib.causality
-- Temporal ordering, causal inference, event relationships.
-- How the substrate knows what caused what.
-- ============================================================

-- ===== LAMPORT CLOCKS =====

-- Lamport timestamp on local event
CREATE OR REPLACE FUNCTION substrate.lamport_local(current_ts BIGINT)
RETURNS BIGINT AS $$ SELECT current_ts + 1 $$ LANGUAGE sql IMMUTABLE;

-- Lamport timestamp on receive: max(local, received) + 1
CREATE OR REPLACE FUNCTION substrate.lamport_recv(local_ts BIGINT, received_ts BIGINT)
RETURNS BIGINT AS $$ SELECT GREATEST(local_ts, received_ts) + 1 $$ LANGUAGE sql IMMUTABLE;

-- Hybrid logical clock: physical + logical counter
CREATE OR REPLACE FUNCTION substrate.hlc_tick(
    local_pt BIGINT, local_lc INT,
    msg_pt BIGINT DEFAULT 0, msg_lc INT DEFAULT 0,
    wall_clock BIGINT DEFAULT 0
)
RETURNS BIGINT[] AS $$
# Returns [physical_time, logical_counter]
pt = max(local_pt, msg_pt, wall_clock)
if pt == local_pt and pt == msg_pt:
    lc = max(local_lc, msg_lc) + 1
elif pt == local_pt:
    lc = local_lc + 1
elif pt == msg_pt:
    lc = msg_lc + 1
else:
    lc = 0
return [pt, lc]
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== HAPPENS-BEFORE =====

-- Check happens-before from vector clocks: does event A happen before B?
CREATE OR REPLACE FUNCTION substrate.happens_before(vc_a JSONB, vc_b JSONB)
RETURNS BOOLEAN AS $$
import json
a, b = json.loads(vc_a), json.loads(vc_b)
all_keys = set(a) | set(b)
at_least_one_less = False
for k in all_keys:
    va, vb = a.get(k, 0), b.get(k, 0)
    if va > vb: return False
    if va < vb: at_least_one_less = True
return at_least_one_less
$$ LANGUAGE plpython3u IMMUTABLE;

-- Are two events concurrent? (neither happens-before the other)
CREATE OR REPLACE FUNCTION substrate.is_concurrent(vc_a JSONB, vc_b JSONB)
RETURNS BOOLEAN AS $$
import json
a, b = json.loads(vc_a), json.loads(vc_b)
all_keys = set(a) | set(b)
a_le_b = all(a.get(k,0) <= b.get(k,0) for k in all_keys)
b_le_a = all(b.get(k,0) <= a.get(k,0) for k in all_keys)
return not a_le_b and not b_le_a
$$ LANGUAGE plpython3u IMMUTABLE;

-- Causal order: sort events by vector clocks (topological sort of partial order)
CREATE OR REPLACE FUNCTION substrate.causal_order(events JSONB)
RETURNS JSONB AS $$
import json
evts = json.loads(events)  # [{id, vclock: {node: counter}}]
n = len(evts)
def hb(a, b):
    ak, bk = set(a) | set(b), set(a) | set(b)
    all_le = all(a.get(k,0) <= b.get(k,0) for k in ak | bk)
    strict = any(a.get(k,0) < b.get(k,0) for k in ak | bk)
    return all_le and strict
# Build DAG
dag = {i: [] for i in range(n)}
indeg = [0] * n
for i in range(n):
    for j in range(n):
        if i != j and hb(evts[i].get('vclock',{}), evts[j].get('vclock',{})):
            # Check if direct (no intermediate)
            direct = True
            for k in range(n):
                if k != i and k != j:
                    if hb(evts[i].get('vclock',{}), evts[k].get('vclock',{})) and hb(evts[k].get('vclock',{}), evts[j].get('vclock',{})):
                        direct = False
                        break
            if direct:
                dag[i].append(j)
                indeg[j] += 1
# Kahn's algorithm
from collections import deque
queue = deque(i for i in range(n) if indeg[i] == 0)
order = []
while queue:
    u = queue.popleft()
    order.append(evts[u].get('id', str(u)))
    for v in dag[u]:
        indeg[v] -= 1
        if indeg[v] == 0: queue.append(v)
return json.dumps({'ordered': order, 'total': n})
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== CAUSAL INFERENCE =====

-- Granger causality test statistic (simplified: does X's past predict Y better?)
-- Takes two time series, returns F-statistic approximation
CREATE OR REPLACE FUNCTION substrate.granger_test(x FLOAT8[], y FLOAT8[], lag INT DEFAULT 1)
RETURNS JSONB AS $$
import json
n = min(len(x), len(y))
if n <= 2 * lag: return json.dumps({'error': 'series too short for lag'})
# Restricted model: Y predicted by its own past
y_actual = y[lag:n]
y_pred_r = y[:n-lag]
sse_r = sum((a - p)**2 for a, p in zip(y_actual, y_pred_r))
# Unrestricted model: Y predicted by its own past + X's past
# Simple: average of lagged Y and lagged X
y_pred_u = [(y[i] + x[i]) / 2 for i in range(n - lag)]
sse_u = sum((a - p)**2 for a, p in zip(y_actual, y_pred_u))
# F-statistic
df = n - 2 * lag
if sse_u == 0: f_stat = float('inf')
elif df <= 0: f_stat = 0
else: f_stat = ((sse_r - sse_u) / lag) / (sse_u / df)
return json.dumps({
    'f_statistic': round(f_stat, 4),
    'sse_restricted': round(sse_r, 4),
    'sse_unrestricted': round(sse_u, 4),
    'likely_causal': f_stat > 3.84,  # ~p<0.05 for 1 df
    'lag': lag
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Transfer entropy: information flow X -> Y
CREATE OR REPLACE FUNCTION substrate.transfer_entropy(x FLOAT8[], y FLOAT8[], n_bins INT DEFAULT 4)
RETURNS FLOAT8 AS $$
import math
from collections import Counter
n = min(len(x), len(y)) - 1
if n < 10: return 0
# Discretize
def bin_val(v, vals):
    sorted_v = sorted(vals)
    step = len(sorted_v) // n_bins
    for i in range(n_bins - 1):
        if v <= sorted_v[min((i+1)*step, len(sorted_v)-1)]:
            return i
    return n_bins - 1
xb = [bin_val(v, x) for v in x[:n+1]]
yb = [bin_val(v, y) for v in y[:n+1]]
# Count joint occurrences: (y_t+1, y_t, x_t)
joint = Counter()
yx = Counter()
yy = Counter()
yonly = Counter()
for t in range(n):
    joint[(yb[t+1], yb[t], xb[t])] += 1
    yx[(yb[t], xb[t])] += 1
    yy[(yb[t+1], yb[t])] += 1
    yonly[yb[t]] += 1
te = 0
for (yt1, yt, xt), count in joint.items():
    p_joint = count / n
    p_yt1_given_yt_xt = count / yx[(yt, xt)] if yx[(yt, xt)] > 0 else 0
    p_yt1_given_yt = yy[(yt1, yt)] / yonly[yt] if yonly[yt] > 0 else 0
    if p_yt1_given_yt_xt > 0 and p_yt1_given_yt > 0:
        te += p_joint * math.log2(p_yt1_given_yt_xt / p_yt1_given_yt)
return te
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== INTERVAL TEMPORAL LOGIC =====

-- Allen's interval relations: classify relationship between two intervals
CREATE OR REPLACE FUNCTION substrate.allen_relation(a_start FLOAT8, a_end FLOAT8, b_start FLOAT8, b_end FLOAT8)
RETURNS TEXT AS $$
if a_end < b_start: return 'before'
if a_end == b_start: return 'meets'
if a_start < b_start and a_end > b_start and a_end < b_end: return 'overlaps'
if a_start == b_start and a_end < b_end: return 'starts'
if a_start > b_start and a_end < b_end: return 'during'
if a_start > b_start and a_end == b_end: return 'finishes'
if a_start == b_start and a_end == b_end: return 'equal'
if a_start > b_start and a_start < b_end and a_end > b_end: return 'overlapped_by'
if a_start == b_end: return 'met_by'
if a_start > b_end: return 'after'
if a_start == b_start and a_end > b_end: return 'started_by'
if a_start < b_start and a_end > b_end: return 'contains'
if a_start < b_start and a_end == b_end: return 'finished_by'
return 'unknown'
$$ LANGUAGE plpython3u IMMUTABLE;

-- Do two intervals overlap?
CREATE OR REPLACE FUNCTION substrate.intervals_overlap(a_start FLOAT8, a_end FLOAT8, b_start FLOAT8, b_end FLOAT8)
RETURNS BOOLEAN AS $$ SELECT a_start < b_end AND b_start < a_end $$ LANGUAGE sql IMMUTABLE;

-- Interval intersection (returns NULL if no overlap)
CREATE OR REPLACE FUNCTION substrate.interval_intersect(a_start FLOAT8, a_end FLOAT8, b_start FLOAT8, b_end FLOAT8)
RETURNS FLOAT8[] AS $$
lo = max(a_start, b_start)
hi = min(a_end, b_end)
if lo >= hi: return None
return [lo, hi]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Interval union (returns enclosing interval)
CREATE OR REPLACE FUNCTION substrate.interval_union(a_start FLOAT8, a_end FLOAT8, b_start FLOAT8, b_end FLOAT8)
RETURNS FLOAT8[] AS $$ SELECT ARRAY[LEAST(a_start, b_start), GREATEST(a_end, b_end)] $$ LANGUAGE sql IMMUTABLE;

-- ===== EVENT DAG =====

-- Build causal DAG from events with parent references
-- Events: [{id, parents: [id, ...], timestamp}]
CREATE OR REPLACE FUNCTION substrate.build_causal_dag(events JSONB)
RETURNS JSONB AS $$
import json
from collections import defaultdict
evts = json.loads(events)
children = defaultdict(list)
roots = []
all_ids = set()
for e in evts:
    eid = e['id']
    all_ids.add(eid)
    parents = e.get('parents', [])
    if not parents:
        roots.append(eid)
    for p in parents:
        children[p].append(eid)
# Compute depth
depth = {}
def get_depth(eid):
    if eid in depth: return depth[eid]
    evt = next((e for e in evts if e['id'] == eid), None)
    if not evt or not evt.get('parents'):
        depth[eid] = 0
        return 0
    d = max(get_depth(p) for p in evt['parents']) + 1
    depth[eid] = d
    return d
for e in evts:
    get_depth(e['id'])
return json.dumps({
    'roots': roots,
    'max_depth': max(depth.values()) if depth else 0,
    'n_events': len(evts),
    'depths': depth
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Lowest common ancestor in a DAG (for two events)
CREATE OR REPLACE FUNCTION substrate.lca(events JSONB, event_a TEXT, event_b TEXT)
RETURNS TEXT AS $$
import json
from collections import defaultdict
evts = {e['id']: e for e in json.loads(events)}
def ancestors(eid):
    result = set()
    stack = [eid]
    while stack:
        n = stack.pop()
        if n in result: continue
        result.add(n)
        if n in evts:
            stack.extend(evts[n].get('parents', []))
    return result
a_anc = ancestors(event_a)
b_anc = ancestors(event_b)
common = a_anc & b_anc
if not common: return None
# Find the deepest common ancestor
def depth(eid, memo={}):
    if eid in memo: return memo[eid]
    if eid not in evts or not evts[eid].get('parents'):
        memo[eid] = 0
        return 0
    d = max(depth(p) for p in evts[eid]['parents']) + 1
    memo[eid] = d
    return d
return max(common, key=lambda x: depth(x))
$$ LANGUAGE plpython3u IMMUTABLE;
