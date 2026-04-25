-- ============================================================
-- SUBSTRATE LIBRARY: lib.routing
-- Mesh routing, topology, peer selection, consensus, CRDTs
-- ============================================================

-- ===== GRAPH / TOPOLOGY =====

-- Dijkstra shortest path: edges as JSON array [{from,to,weight}], returns path + cost
CREATE OR REPLACE FUNCTION substrate.shortest_path(edges JSONB, source TEXT, target TEXT)
RETURNS JSONB AS $$
import json, heapq
graph = {}
for e in json.loads(edges):
    f, t, w = e['from'], e['to'], e['weight']
    graph.setdefault(f, []).append((t, w))
    graph.setdefault(t, []).append((f, w))  # undirected
dist = {source: 0}
prev = {}
heap = [(0, source)]
while heap:
    d, u = heapq.heappop(heap)
    if d > dist.get(u, float('inf')): continue
    if u == target: break
    for v, w in graph.get(u, []):
        nd = d + w
        if nd < dist.get(v, float('inf')):
            dist[v] = nd
            prev[v] = u
            heapq.heappush(heap, (nd, v))
if target not in dist:
    return json.dumps({'reachable': False, 'cost': None, 'path': []})
path = []
n = target
while n is not None:
    path.append(n)
    n = prev.get(n)
path.reverse()
return json.dumps({'reachable': True, 'cost': dist[target], 'path': path})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Minimum spanning tree (Kruskal): edges [{from,to,weight}]
CREATE OR REPLACE FUNCTION substrate.min_spanning_tree(edges JSONB)
RETURNS JSONB AS $$
import json
edge_list = sorted(json.loads(edges), key=lambda e: e['weight'])
parent = {}
def find(x):
    while parent.get(x, x) != x:
        parent[x] = parent.get(parent[x], parent[x])
        x = parent[x]
    return x
def union(a, b):
    ra, rb = find(a), find(b)
    if ra != rb: parent[ra] = rb; return True
    return False
mst = []
total = 0
for e in edge_list:
    if union(e['from'], e['to']):
        mst.append(e)
        total += e['weight']
return json.dumps({'edges': mst, 'total_weight': total, 'edge_count': len(mst)})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Graph degree centrality: node with most connections
CREATE OR REPLACE FUNCTION substrate.degree_centrality(edges JSONB)
RETURNS JSONB AS $$
import json
from collections import Counter
counts = Counter()
for e in json.loads(edges):
    counts[e['from']] += 1
    counts[e['to']] += 1
ranked = sorted(counts.items(), key=lambda x: -x[1])
return json.dumps([{'node': n, 'degree': d} for n, d in ranked])
$$ LANGUAGE plpython3u IMMUTABLE;

-- Betweenness centrality approximation (BFS-based)
CREATE OR REPLACE FUNCTION substrate.betweenness_centrality(edges JSONB)
RETURNS JSONB AS $$
import json
from collections import defaultdict, deque
graph = defaultdict(set)
for e in json.loads(edges):
    graph[e['from']].add(e['to'])
    graph[e['to']].add(e['from'])
nodes = list(graph.keys())
centrality = {n: 0.0 for n in nodes}
for s in nodes:
    stack = []; pred = {n: [] for n in nodes}
    sigma = {n: 0.0 for n in nodes}; sigma[s] = 1.0
    dist = {n: -1 for n in nodes}; dist[s] = 0
    queue = deque([s])
    while queue:
        v = queue.popleft(); stack.append(v)
        for w in graph[v]:
            if dist[w] < 0:
                queue.append(w); dist[w] = dist[v] + 1
            if dist[w] == dist[v] + 1:
                sigma[w] += sigma[v]; pred[w].append(v)
    delta = {n: 0.0 for n in nodes}
    while stack:
        w = stack.pop()
        for v in pred[w]:
            delta[v] += (sigma[v] / sigma[w]) * (1 + delta[w])
        if w != s:
            centrality[w] += delta[w]
ranked = sorted(centrality.items(), key=lambda x: -x[1])
return json.dumps([{'node': n, 'centrality': round(c, 4)} for n, c in ranked[:20]])
$$ LANGUAGE plpython3u IMMUTABLE;

-- Network diameter: longest shortest path between any two nodes
CREATE OR REPLACE FUNCTION substrate.network_diameter(edges JSONB)
RETURNS INT AS $$
import json
from collections import defaultdict, deque
graph = defaultdict(set)
for e in json.loads(edges):
    graph[e['from']].add(e['to'])
    graph[e['to']].add(e['from'])
nodes = list(graph.keys())
diameter = 0
for s in nodes:
    dist = {s: 0}; queue = deque([s])
    while queue:
        v = queue.popleft()
        for w in graph[v]:
            if w not in dist:
                dist[w] = dist[v] + 1; queue.append(w)
                diameter = max(diameter, dist[w])
return diameter
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== PEER SELECTION / LOAD BALANCING =====

-- Weighted random selection: items [{id,weight}], returns chosen id
CREATE OR REPLACE FUNCTION substrate.weighted_select(items JSONB)
RETURNS TEXT AS $$
import json, random
entries = json.loads(items)
total = sum(e['weight'] for e in entries)
r = random.uniform(0, total)
running = 0
for e in entries:
    running += e['weight']
    if r <= running:
        return e['id']
return entries[-1]['id']
$$ LANGUAGE plpython3u;

-- Power of two choices: pick 2 random from pool, choose least loaded
CREATE OR REPLACE FUNCTION substrate.p2c_select(peers JSONB)
RETURNS TEXT AS $$
import json, random
entries = json.loads(peers)  # [{id, load}]
if len(entries) <= 2:
    return min(entries, key=lambda x: x['load'])['id']
a, b = random.sample(entries, 2)
return a['id'] if a['load'] <= b['load'] else b['id']
$$ LANGUAGE plpython3u;

-- Weighted round robin schedule: given weights, produce rotation order
CREATE OR REPLACE FUNCTION substrate.wrr_schedule(peers JSONB)
RETURNS TEXT[] AS $$
import json
from math import gcd
from functools import reduce
entries = json.loads(peers)  # [{id, weight}]
weights = [e['weight'] for e in entries]
g = reduce(gcd, weights)
normalized = [w // g for w in weights]
schedule = []
for i, e in enumerate(entries):
    schedule.extend([e['id']] * normalized[i])
return schedule
$$ LANGUAGE plpython3u IMMUTABLE;

-- Rendezvous hashing (HRW): deterministic peer selection for a key
CREATE OR REPLACE FUNCTION substrate.rendezvous_hash(key TEXT, peers TEXT[])
RETURNS TEXT AS $$
import hashlib
best_peer = None
best_hash = -1
for peer in peers:
    h = int(hashlib.sha256(f'{key}:{peer}'.encode()).hexdigest(), 16)
    if h > best_hash:
        best_hash = h
        best_peer = peer
return best_peer
$$ LANGUAGE plpython3u IMMUTABLE;

-- Consistent hashing with virtual nodes: returns ordered preference list
CREATE OR REPLACE FUNCTION substrate.consistent_hash_ring(key TEXT, peers TEXT[], vnodes INT DEFAULT 150)
RETURNS TEXT[] AS $$
import hashlib
ring = []
for peer in peers:
    for i in range(vnodes):
        h = int(hashlib.md5(f'{peer}:{i}'.encode()).hexdigest(), 16)
        ring.append((h, peer))
ring.sort()
kh = int(hashlib.md5(key.encode()).hexdigest(), 16)
# Walk the ring from kh, collect unique peers in order
seen = set(); result = []
idx = 0
for h, p in ring:
    if h >= kh and p not in seen:
        seen.add(p); result.append(p)
for h, p in ring:
    if p not in seen:
        seen.add(p); result.append(p)
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== CONSENSUS / COORDINATION =====

-- Quorum size for n replicas (strict majority)
CREATE OR REPLACE FUNCTION substrate.quorum_size(n_replicas INT)
RETURNS INT AS $$ SELECT (n_replicas / 2) + 1 $$ LANGUAGE sql IMMUTABLE;

-- Read/write quorum check (R + W > N)
CREATE OR REPLACE FUNCTION substrate.quorum_check(n INT, r INT, w INT)
RETURNS JSONB AS $$
import json
strong = r + w > n
read_your_writes = r + w > n
monotonic = w > n // 2
return json.dumps({
    'N': n, 'R': r, 'W': w,
    'strong_consistency': strong,
    'read_your_writes': read_your_writes,
    'monotonic_writes': monotonic,
    'linearizable': r + w > n and w > n // 2
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Vector clock merge
CREATE OR REPLACE FUNCTION substrate.vclock_merge(a JSONB, b JSONB)
RETURNS JSONB AS $$
import json
va, vb = json.loads(a), json.loads(b)
merged = dict(va)
for k, v in vb.items():
    merged[k] = max(merged.get(k, 0), v)
return json.dumps(merged)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Vector clock increment for a node
CREATE OR REPLACE FUNCTION substrate.vclock_tick(clock JSONB, node_id TEXT)
RETURNS JSONB AS $$
import json
vc = json.loads(clock)
vc[node_id] = vc.get(node_id, 0) + 1
return json.dumps(vc)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Vector clock comparison: returns 'before', 'after', 'concurrent', or 'equal'
CREATE OR REPLACE FUNCTION substrate.vclock_compare(a JSONB, b JSONB)
RETURNS TEXT AS $$
import json
va, vb = json.loads(a), json.loads(b)
all_keys = set(va) | set(vb)
a_le_b = all(va.get(k, 0) <= vb.get(k, 0) for k in all_keys)
b_le_a = all(vb.get(k, 0) <= va.get(k, 0) for k in all_keys)
if a_le_b and b_le_a: return 'equal'
if a_le_b: return 'before'
if b_le_a: return 'after'
return 'concurrent'
$$ LANGUAGE plpython3u IMMUTABLE;

-- CRDT: G-Counter merge
CREATE OR REPLACE FUNCTION substrate.gcounter_merge(a JSONB, b JSONB)
RETURNS JSONB AS $$
import json
va, vb = json.loads(a), json.loads(b)
merged = dict(va)
for k, v in vb.items():
    merged[k] = max(merged.get(k, 0), v)
return json.dumps(merged)
$$ LANGUAGE plpython3u IMMUTABLE;

-- CRDT: G-Counter value (sum of all entries)
CREATE OR REPLACE FUNCTION substrate.gcounter_value(counter JSONB)
RETURNS BIGINT AS $$
import json
return sum(json.loads(counter).values())
$$ LANGUAGE plpython3u IMMUTABLE;

-- CRDT: PN-Counter merge (positive + negative G-Counters)
CREATE OR REPLACE FUNCTION substrate.pncounter_merge(a JSONB, b JSONB)
RETURNS JSONB AS $$
import json
va, vb = json.loads(a), json.loads(b)
merged = {'p': {}, 'n': {}}
for k in set(va.get('p',{})) | set(vb.get('p',{})):
    merged['p'][k] = max(va.get('p',{}).get(k,0), vb.get('p',{}).get(k,0))
for k in set(va.get('n',{})) | set(vb.get('n',{})):
    merged['n'][k] = max(va.get('n',{}).get(k,0), vb.get('n',{}).get(k,0))
return json.dumps(merged)
$$ LANGUAGE plpython3u IMMUTABLE;

-- CRDT: LWW-Register merge (last-writer-wins by timestamp)
CREATE OR REPLACE FUNCTION substrate.lww_merge(a JSONB, b JSONB)
RETURNS JSONB AS $$
import json
va, vb = json.loads(a), json.loads(b)
# Each is {value, timestamp}
if va.get('timestamp', 0) >= vb.get('timestamp', 0):
    return json.dumps(va)
return json.dumps(vb)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== GOSSIP =====

-- Gossip fanout recommendation: ln(N) + 1
CREATE OR REPLACE FUNCTION substrate.gossip_fanout(n_peers INT)
RETURNS INT AS $$
import math
return max(1, int(math.log(max(1, n_peers)) + 1))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Gossip convergence rounds: log2(N)
CREATE OR REPLACE FUNCTION substrate.gossip_rounds(n_peers INT)
RETURNS INT AS $$
import math
return max(1, int(math.ceil(math.log2(max(2, n_peers)))))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Gossip infection probability after k rounds with fanout f in n peers
CREATE OR REPLACE FUNCTION substrate.gossip_coverage(n_peers INT, fanout INT, rounds INT)
RETURNS FLOAT8 AS $$
# Approximate: each round, fraction of uninformed shrinks by (1 - f/n)
uninfected = 1.0
for _ in range(rounds):
    uninfected *= (1 - fanout / n_peers) ** (n_peers * (1 - uninfected))
return 1 - uninfected
$$ LANGUAGE plpython3u IMMUTABLE;

-- SWIM protocol suspect timeout
CREATE OR REPLACE FUNCTION substrate.swim_suspect_timeout(
    protocol_period_ms INT, suspect_multiplier INT DEFAULT 3
)
RETURNS INT AS $$ SELECT protocol_period_ms * suspect_multiplier $$ LANGUAGE sql IMMUTABLE;

-- ===== SPLIT BRAIN =====

-- Split-brain detector: given visible peers and total known peers
CREATE OR REPLACE FUNCTION substrate.split_brain_check(visible_peers INT, total_peers INT)
RETURNS JSONB AS $$
import json
quorum = total_peers // 2 + 1
has_quorum = visible_peers >= quorum
partition_risk = visible_peers < total_peers
return json.dumps({
    'visible': visible_peers,
    'total': total_peers,
    'quorum_needed': quorum,
    'has_quorum': has_quorum,
    'partition_detected': partition_risk and not has_quorum,
    'action': 'operate' if has_quorum else 'fence'
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Sloppy quorum: hinted handoff target selection
CREATE OR REPLACE FUNCTION substrate.hinted_handoff_target(
    failed_peer TEXT, preference_list TEXT[], exclude TEXT[] DEFAULT ARRAY[]::TEXT[]
)
RETURNS TEXT AS $$
for p in preference_list:
    if p != failed_peer and p not in (exclude or []):
        return p
return None
$$ LANGUAGE plpython3u IMMUTABLE;
