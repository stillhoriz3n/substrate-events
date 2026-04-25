-- ============================================================
-- SUBSTRATE LIBRARY: lib.topology
-- The shape of things: clustering, dimensionality, connectivity,
-- community structure, geometric invariants.
-- ============================================================

-- ===== CLUSTERING =====

-- K-means (1D): partition array into k clusters, return centroids + assignments
CREATE OR REPLACE FUNCTION substrate.kmeans_1d(data FLOAT8[], k INT DEFAULT 3, max_iters INT DEFAULT 50)
RETURNS JSONB AS $$
import json, random
if not data or k <= 0: return json.dumps({'centroids':[],'assignments':[]})
pts = list(data)
centroids = sorted(random.sample(pts, min(k, len(pts))))
for _ in range(max_iters):
    assignments = [min(range(len(centroids)), key=lambda c: abs(p - centroids[c])) for p in pts]
    new_centroids = []
    for c in range(len(centroids)):
        members = [pts[i] for i in range(len(pts)) if assignments[i] == c]
        new_centroids.append(sum(members)/len(members) if members else centroids[c])
    if new_centroids == centroids: break
    centroids = new_centroids
return json.dumps({'centroids':[round(c,4) for c in centroids],'assignments':assignments})
$$ LANGUAGE plpython3u;

-- DBSCAN-like density clustering (1D)
CREATE OR REPLACE FUNCTION substrate.density_clusters(data FLOAT8[], eps FLOAT8, min_pts INT DEFAULT 2)
RETURNS JSONB AS $$
import json
pts = sorted(enumerate(data), key=lambda x: x[1])
n = len(pts)
labels = [-1] * n  # -1 = noise
cluster = 0
for i in range(n):
    if labels[pts[i][0]] != -1: continue
    neighbors = [j for j in range(n) if abs(pts[j][1] - pts[i][1]) <= eps]
    if len(neighbors) < min_pts: continue
    labels[pts[i][0]] = cluster
    seeds = list(neighbors)
    while seeds:
        q = seeds.pop(0)
        if labels[pts[q][0]] == -1: labels[pts[q][0]] = cluster
        elif labels[pts[q][0]] != -1 and labels[pts[q][0]] != cluster: continue
        else: labels[pts[q][0]] = cluster
        qn = [j for j in range(n) if abs(pts[j][1] - pts[q][1]) <= eps]
        if len(qn) >= min_pts:
            seeds.extend(j for j in qn if labels[pts[j][0]] == -1)
    cluster += 1
return json.dumps({'labels':labels,'n_clusters':cluster,'n_noise':labels.count(-1)})
$$ LANGUAGE plpython3u;

-- Silhouette score (1D clusters): how well-separated are the clusters?
CREATE OR REPLACE FUNCTION substrate.silhouette(data FLOAT8[], labels INT[])
RETURNS FLOAT8 AS $$
n = len(data)
if n < 2: return 0
clusters = set(labels)
if len(clusters) < 2: return 0
scores = []
for i in range(n):
    ci = labels[i]
    same = [j for j in range(n) if labels[j] == ci and j != i]
    if not same:
        scores.append(0)
        continue
    a_i = sum(abs(data[i] - data[j]) for j in same) / len(same)
    b_i = float('inf')
    for ck in clusters:
        if ck == ci: continue
        others = [j for j in range(n) if labels[j] == ck]
        if others:
            b_i = min(b_i, sum(abs(data[i] - data[j]) for j in others) / len(others))
    scores.append((b_i - a_i) / max(a_i, b_i) if max(a_i, b_i) > 0 else 0)
return sum(scores) / len(scores)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== CONNECTIVITY =====

-- Connected components from edge list
CREATE OR REPLACE FUNCTION substrate.connected_components(edges JSONB)
RETURNS JSONB AS $$
import json
from collections import defaultdict, deque
graph = defaultdict(set)
for e in json.loads(edges):
    graph[e['from']].add(e['to'])
    graph[e['to']].add(e['from'])
visited = set()
components = []
for node in graph:
    if node in visited: continue
    comp = []
    queue = deque([node])
    while queue:
        n = queue.popleft()
        if n in visited: continue
        visited.add(n)
        comp.append(n)
        queue.extend(graph[n] - visited)
    components.append(sorted(comp))
components.sort(key=len, reverse=True)
return json.dumps({'n_components': len(components), 'components': components})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Graph density: 2E / (V * (V-1)) for undirected
CREATE OR REPLACE FUNCTION substrate.graph_density(n_vertices INT, n_edges INT)
RETURNS FLOAT8 AS $$ SELECT (2.0 * n_edges) / (n_vertices::float8 * (n_vertices - 1)) $$ LANGUAGE sql IMMUTABLE;

-- Clustering coefficient of a node: fraction of neighbors that are connected
CREATE OR REPLACE FUNCTION substrate.clustering_coefficient(edges JSONB, node TEXT)
RETURNS FLOAT8 AS $$
import json
from collections import defaultdict
graph = defaultdict(set)
for e in json.loads(edges):
    graph[e['from']].add(e['to'])
    graph[e['to']].add(e['from'])
neighbors = graph.get(node, set())
k = len(neighbors)
if k < 2: return 0
triangles = 0
nlist = list(neighbors)
for i in range(len(nlist)):
    for j in range(i+1, len(nlist)):
        if nlist[j] in graph[nlist[i]]:
            triangles += 1
return (2.0 * triangles) / (k * (k - 1))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Is graph bipartite? Returns partition or null
CREATE OR REPLACE FUNCTION substrate.is_bipartite(edges JSONB)
RETURNS JSONB AS $$
import json
from collections import defaultdict, deque
graph = defaultdict(set)
for e in json.loads(edges):
    graph[e['from']].add(e['to'])
    graph[e['to']].add(e['from'])
color = {}
for start in graph:
    if start in color: continue
    queue = deque([start])
    color[start] = 0
    while queue:
        n = queue.popleft()
        for nb in graph[n]:
            if nb not in color:
                color[nb] = 1 - color[n]
                queue.append(nb)
            elif color[nb] == color[n]:
                return json.dumps({'bipartite': False})
a = sorted(n for n, c in color.items() if c == 0)
b = sorted(n for n, c in color.items() if c == 1)
return json.dumps({'bipartite': True, 'partition_a': a, 'partition_b': b})
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== DIMENSIONALITY =====

-- Intrinsic dimensionality estimate (correlation dimension via nearest-neighbor)
CREATE OR REPLACE FUNCTION substrate.intrinsic_dim(distances FLOAT8[])
RETURNS FLOAT8 AS $$
import math
d = sorted(distances)
d = [x for x in d if x > 0]
if len(d) < 4: return 1
# Maximum likelihood estimator
n = len(d)
t = d[n//2]  # threshold at median
inside = [x for x in d if x <= t]
if len(inside) < 2 or t <= 0: return 1
return len(inside) / sum(math.log(t/x) for x in inside if x > 0)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Embedding quality: trustworthiness measure
-- How well does a low-dim embedding preserve neighborhoods?
CREATE OR REPLACE FUNCTION substrate.trustworthiness(
    orig_ranks INT[], embed_ranks INT[], k INT
)
RETURNS FLOAT8 AS $$
n = len(orig_ranks)
if n == 0 or k == 0: return 1.0
violations = 0
for i in range(min(n, len(embed_ranks))):
    if embed_ranks[i] <= k and orig_ranks[i] > k:
        violations += orig_ranks[i] - k
max_violations = k * (2*n - 3*k - 1) / 2
if max_violations == 0: return 1.0
return 1 - (2 / (n * max_violations)) * violations
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== COMMUNITY DETECTION =====

-- Label propagation (simplified): iterative community assignment
CREATE OR REPLACE FUNCTION substrate.label_propagation(edges JSONB, max_iters INT DEFAULT 20)
RETURNS JSONB AS $$
import json, random
from collections import defaultdict, Counter
graph = defaultdict(set)
for e in json.loads(edges):
    graph[e['from']].add(e['to'])
    graph[e['to']].add(e['from'])
nodes = list(graph.keys())
labels = {n: i for i, n in enumerate(nodes)}
for _ in range(max_iters):
    changed = False
    random.shuffle(nodes)
    for n in nodes:
        if not graph[n]: continue
        neighbor_labels = Counter(labels[nb] for nb in graph[n])
        best = neighbor_labels.most_common(1)[0][0]
        if labels[n] != best:
            labels[n] = best
            changed = True
    if not changed: break
communities = defaultdict(list)
for n, l in labels.items():
    communities[l].append(n)
result = sorted(communities.values(), key=len, reverse=True)
return json.dumps({'n_communities': len(result), 'communities': result})
$$ LANGUAGE plpython3u;

-- Modularity score: quality of a graph partition
CREATE OR REPLACE FUNCTION substrate.modularity(edges JSONB, communities JSONB)
RETURNS FLOAT8 AS $$
import json
from collections import defaultdict
edge_list = json.loads(edges)
comms = json.loads(communities)  # [[node, ...], ...]
node_to_comm = {}
for i, comm in enumerate(comms):
    for n in comm:
        node_to_comm[n] = i
m = len(edge_list)
if m == 0: return 0
degree = defaultdict(int)
for e in edge_list:
    degree[e['from']] += 1
    degree[e['to']] += 1
Q = 0
for e in edge_list:
    if node_to_comm.get(e['from']) == node_to_comm.get(e['to']):
        Q += 1 - (degree[e['from']] * degree[e['to']]) / (2 * m)
return Q / m
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== GEOMETRIC INVARIANTS =====

-- Euler characteristic: V - E + F
CREATE OR REPLACE FUNCTION substrate.euler_characteristic(vertices INT, edges_count INT, faces INT)
RETURNS INT AS $$ SELECT vertices - edges_count + faces $$ LANGUAGE sql IMMUTABLE;

-- Betti numbers for simple structures: connected components, cycles, cavities
CREATE OR REPLACE FUNCTION substrate.betti_numbers(n_components INT, n_independent_cycles INT, n_cavities INT DEFAULT 0)
RETURNS INT[] AS $$ SELECT ARRAY[n_components, n_independent_cycles, n_cavities] $$ LANGUAGE sql IMMUTABLE;

-- Graph cycle rank (independent cycles): E - V + C (components)
CREATE OR REPLACE FUNCTION substrate.cycle_rank(n_vertices INT, n_edges INT, n_components INT DEFAULT 1)
RETURNS INT AS $$ SELECT n_edges - n_vertices + n_components $$ LANGUAGE sql IMMUTABLE;

-- Small world coefficient: high clustering + low path length
CREATE OR REPLACE FUNCTION substrate.small_world_coeff(
    actual_clustering FLOAT8, random_clustering FLOAT8,
    actual_path_len FLOAT8, random_path_len FLOAT8
)
RETURNS FLOAT8 AS $$
if random_clustering == 0 or actual_path_len == 0: return 0
return (actual_clustering / random_clustering) / (actual_path_len / random_path_len)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Scale-free test: does degree distribution follow power law?
CREATE OR REPLACE FUNCTION substrate.power_law_fit(degrees INT[])
RETURNS JSONB AS $$
import json, math
from collections import Counter
counts = Counter(degrees)
if not counts: return json.dumps({'is_power_law': False})
x = sorted(counts.keys())
x = [k for k in x if k > 0]
if len(x) < 3: return json.dumps({'is_power_law': False, 'reason': 'too few unique degrees'})
# MLE for power law exponent
xmin = x[0]
filtered = [d for d in degrees if d >= xmin]
n = len(filtered)
alpha = 1 + n / sum(math.log(d / (xmin - 0.5)) for d in filtered)
return json.dumps({
    'alpha': round(alpha, 3),
    'xmin': xmin,
    'is_power_law': 1.5 < alpha < 3.5,
    'n_samples': n
})
$$ LANGUAGE plpython3u IMMUTABLE;
