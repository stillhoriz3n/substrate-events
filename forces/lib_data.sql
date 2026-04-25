-- ============================================================
-- SUBSTRATE LIBRARY: lib.data
-- Data structures, encoding, compression, estimation
-- ============================================================

-- JSON path extract (dot-notation: "a.b.c" from jsonb)
CREATE OR REPLACE FUNCTION substrate.jpath(doc JSONB, path TEXT)
RETURNS JSONB AS $$
import json
obj = json.loads(doc)
for key in path.split('.'):
    if isinstance(obj, dict):
        obj = obj.get(key)
    elif isinstance(obj, list):
        try: obj = obj[int(key)]
        except: obj = None
    else:
        return None
    if obj is None: return None
return json.dumps(obj)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Flatten nested JSON to dot-notation keys
CREATE OR REPLACE FUNCTION substrate.json_flatten(doc JSONB)
RETURNS JSONB AS $$
import json
def flatten(obj, prefix=''):
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f'{prefix}.{k}' if prefix else k
            items.update(flatten(v, new_key))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            items.update(flatten(v, f'{prefix}.{i}'))
    else:
        items[prefix] = obj
    return items
return json.dumps(flatten(json.loads(doc)))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Deep merge two JSONB objects (b overwrites a)
CREATE OR REPLACE FUNCTION substrate.json_merge(a JSONB, b JSONB)
RETURNS JSONB AS $$
import json
def merge(x, y):
    if isinstance(x, dict) and isinstance(y, dict):
        result = dict(x)
        for k, v in y.items():
            result[k] = merge(x.get(k), v) if k in x else v
        return result
    return y
return json.dumps(merge(json.loads(a), json.loads(b)))
$$ LANGUAGE plpython3u IMMUTABLE;

-- JSON diff: returns keys that differ between two objects
CREATE OR REPLACE FUNCTION substrate.json_diff(a JSONB, b JSONB)
RETURNS JSONB AS $$
import json
def flatten(obj, prefix=''):
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            items.update(flatten(v, f'{prefix}.{k}' if prefix else k))
    else:
        items[prefix] = obj
    return items
fa, fb = flatten(json.loads(a)), flatten(json.loads(b))
diff = {}
for k in set(fa) | set(fb):
    va, vb = fa.get(k), fb.get(k)
    if va != vb:
        diff[k] = {'old': va, 'new': vb}
return json.dumps(diff)
$$ LANGUAGE plpython3u IMMUTABLE;

-- HyperLogLog cardinality estimator (approx count distinct)
CREATE OR REPLACE FUNCTION substrate.hll_estimate(vals TEXT[])
RETURNS FLOAT8 AS $$
import hashlib, math
m = 1024  # 2^10 buckets
registers = [0] * m
for v in vals:
    h = int(hashlib.md5(v.encode()).hexdigest(), 16)
    bucket = h & (m - 1)
    bits = h >> 10
    registers[bucket] = max(registers[bucket], len(bin(bits | (1<<64))) - len(bin(bits | (1<<64)).rstrip('0')))
alpha = 0.7213 / (1 + 1.079 / m)
raw = alpha * m * m / sum(2**(-r) for r in registers)
if raw <= 2.5 * m:
    zeros = registers.count(0)
    if zeros > 0:
        raw = m * math.log(m / zeros)
return raw
$$ LANGUAGE plpython3u IMMUTABLE;

-- Bloom filter check (probabilistic set membership)
-- Creates a bloom filter from items, tests if candidate might be in set
CREATE OR REPLACE FUNCTION substrate.bloom_check(items TEXT[], candidate TEXT, fp_rate FLOAT8 DEFAULT 0.01)
RETURNS BOOLEAN AS $$
import hashlib, math
n = len(items)
if n == 0: return False
m = int(-n * math.log(fp_rate) / (math.log(2)**2))
k = max(1, int(m / n * math.log(2)))
bits = [False] * m
def hashes(val):
    h1 = int(hashlib.md5(val.encode()).hexdigest(), 16)
    h2 = int(hashlib.sha1(val.encode()).hexdigest(), 16)
    return [(h1 + i * h2) % m for i in range(k)]
for item in items:
    for pos in hashes(item):
        bits[pos] = True
return all(bits[pos] for pos in hashes(candidate))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Count-min sketch frequency estimate
CREATE OR REPLACE FUNCTION substrate.cms_estimate(items TEXT[], query TEXT, width INT DEFAULT 1000, depth INT DEFAULT 5)
RETURNS INT AS $$
import hashlib
table = [[0]*width for _ in range(depth)]
def get_hash(val, i):
    return int(hashlib.md5(f'{i}:{val}'.encode()).hexdigest(), 16) % width
for item in items:
    for i in range(depth):
        table[i][get_hash(item, i)] += 1
return min(table[i][get_hash(query, i)] for i in range(depth))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Run-length encode an array
CREATE OR REPLACE FUNCTION substrate.rle_encode(arr TEXT[])
RETURNS TEXT AS $$
import json
if not arr: return '[]'
result = []
count = 1
for i in range(1, len(arr)):
    if arr[i] == arr[i-1]:
        count += 1
    else:
        result.append([arr[i-1], count])
        count = 1
result.append([arr[-1], count])
return json.dumps(result)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Reservoir sampling: pick k random items from array
CREATE OR REPLACE FUNCTION substrate.sample(arr TEXT[], k INT DEFAULT 10)
RETURNS TEXT[] AS $$
import random
return random.sample(arr, min(k, len(arr)))
$$ LANGUAGE plpython3u;

-- Consistent hash ring: given a key and a list of nodes, return the node
CREATE OR REPLACE FUNCTION substrate.hash_ring(key TEXT, nodes TEXT[], replicas INT DEFAULT 100)
RETURNS TEXT AS $$
import hashlib
ring = {}
for node in nodes:
    for i in range(replicas):
        h = int(hashlib.md5(f'{node}:{i}'.encode()).hexdigest(), 16)
        ring[h] = node
sorted_keys = sorted(ring.keys())
kh = int(hashlib.md5(key.encode()).hexdigest(), 16)
for sk in sorted_keys:
    if sk >= kh:
        return ring[sk]
return ring[sorted_keys[0]]
$$ LANGUAGE plpython3u IMMUTABLE;
