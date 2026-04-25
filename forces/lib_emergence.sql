-- ============================================================
-- SUBSTRATE LIBRARY: lib.emergence
-- Complexity, self-organization, phase transitions, attractors.
-- How simple rules produce complex behavior.
-- ============================================================

-- ===== POWER LAWS & DISTRIBUTIONS =====

-- Zipf's law: expected frequency of rank r item given N items and exponent s
CREATE OR REPLACE FUNCTION substrate.zipf(rank INT, n_items INT, exponent FLOAT8 DEFAULT 1.0)
RETURNS FLOAT8 AS $$
import math
harmonic = sum(1.0 / (k ** exponent) for k in range(1, n_items + 1))
return (1.0 / (rank ** exponent)) / harmonic
$$ LANGUAGE plpython3u IMMUTABLE;

-- Pareto distribution: P(X > x) = (x_min / x)^alpha
CREATE OR REPLACE FUNCTION substrate.pareto_survival(x FLOAT8, x_min FLOAT8, alpha FLOAT8)
RETURNS FLOAT8 AS $$
if x < x_min: return 1.0
return (x_min / x) ** alpha
$$ LANGUAGE plpython3u IMMUTABLE;

-- Power law exponent estimation (Hill estimator)
CREATE OR REPLACE FUNCTION substrate.hill_estimator(data FLOAT8[], x_min FLOAT8 DEFAULT 0)
RETURNS FLOAT8 AS $$
import math
filtered = sorted([x for x in data if x > x_min], reverse=True)
if len(filtered) < 5: return 0
if x_min <= 0: x_min = filtered[-1]
n = len(filtered)
return 1 + n / sum(math.log(x / x_min) for x in filtered if x > x_min)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Herfindahl-Hirschman Index: concentration measure (0..1 normalized)
CREATE OR REPLACE FUNCTION substrate.hhi(shares FLOAT8[])
RETURNS FLOAT8 AS $$
total = sum(shares)
if total == 0: return 0
normed = [s / total for s in shares]
return sum(s * s for s in normed)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Gini coefficient: inequality measure [0=equal, 1=concentrated]
CREATE OR REPLACE FUNCTION substrate.gini(data FLOAT8[])
RETURNS FLOAT8 AS $$
s = sorted(data)
n = len(s)
if n == 0 or sum(s) == 0: return 0
numer = sum((2*i - n - 1) * s[i] for i in range(n))
denom = n * sum(s)
return numer / denom
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== DYNAMICAL SYSTEMS =====

-- Logistic map: x_{n+1} = r * x_n * (1 - x_n)
CREATE OR REPLACE FUNCTION substrate.logistic_map(x0 FLOAT8, r FLOAT8, n_steps INT DEFAULT 100)
RETURNS FLOAT8[] AS $$
result = [x0]
x = x0
for _ in range(n_steps - 1):
    x = r * x * (1 - x)
    result.append(x)
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- Lyapunov exponent for logistic map (positive = chaos)
CREATE OR REPLACE FUNCTION substrate.lyapunov_logistic(r FLOAT8, n_iters INT DEFAULT 1000, n_transient INT DEFAULT 200)
RETURNS FLOAT8 AS $$
import math
x = 0.5
for _ in range(n_transient):
    x = r * x * (1 - x)
total = 0
for _ in range(n_iters):
    x = r * x * (1 - x)
    deriv = abs(r * (1 - 2*x))
    if deriv > 0:
        total += math.log(deriv)
return total / n_iters
$$ LANGUAGE plpython3u IMMUTABLE;

-- Tent map: x_{n+1} = mu * min(x, 1-x)
CREATE OR REPLACE FUNCTION substrate.tent_map(x0 FLOAT8, mu FLOAT8 DEFAULT 2.0, n_steps INT DEFAULT 100)
RETURNS FLOAT8[] AS $$
result = [x0]
x = x0
for _ in range(n_steps - 1):
    x = mu * min(x, 1 - x)
    result.append(x)
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- Lorenz attractor step (dx, dy, dz given current state)
CREATE OR REPLACE FUNCTION substrate.lorenz_step(
    x FLOAT8, y FLOAT8, z FLOAT8, dt FLOAT8 DEFAULT 0.01,
    sigma FLOAT8 DEFAULT 10, rho FLOAT8 DEFAULT 28, beta FLOAT8 DEFAULT 2.667
)
RETURNS FLOAT8[] AS $$
dx = sigma * (y - x) * dt
dy = (x * (rho - z) - y) * dt
dz = (x * y - beta * z) * dt
return [x + dx, y + dy, z + dz]
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== CELLULAR AUTOMATA =====

-- Elementary cellular automaton: 1D, 1 step
-- rule: 0-255 (Wolfram numbering), state: array of 0/1
CREATE OR REPLACE FUNCTION substrate.ca_step(state INT[], rule_num INT DEFAULT 110)
RETURNS INT[] AS $$
n = len(state)
result = [0] * n
for i in range(n):
    left = state[(i - 1) % n]
    center = state[i]
    right = state[(i + 1) % n]
    neighborhood = (left << 2) | (center << 1) | right
    result[i] = (rule_num >> neighborhood) & 1
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- Run cellular automaton for n steps, return all states
CREATE OR REPLACE FUNCTION substrate.ca_run(initial INT[], rule_num INT DEFAULT 110, n_steps INT DEFAULT 50)
RETURNS INT[][] AS $$
state = list(initial)
history = [state[:]]
n = len(state)
for _ in range(n_steps):
    new_state = [0] * n
    for i in range(n):
        left = state[(i - 1) % n]
        center = state[i]
        right = state[(i + 1) % n]
        neighborhood = (left << 2) | (center << 1) | right
        new_state[i] = (rule_num >> neighborhood) & 1
    state = new_state
    history.append(state[:])
return history
$$ LANGUAGE plpython3u IMMUTABLE;

-- Game of Life step (1D simplification: totalistic rule)
CREATE OR REPLACE FUNCTION substrate.life_1d_step(state INT[])
RETURNS INT[] AS $$
n = len(state)
result = [0] * n
for i in range(n):
    neighbors = state[(i-1)%n] + state[(i+1)%n]
    if state[i] == 1:
        result[i] = 1 if neighbors in (1, 2) else 0
    else:
        result[i] = 1 if neighbors == 1 else 0
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== COMPLEXITY MEASURES =====

-- Kolmogorov complexity estimate (compressibility proxy)
CREATE OR REPLACE FUNCTION substrate.complexity_estimate(data TEXT)
RETURNS JSONB AS $$
import json, zlib
raw = data.encode('utf-8')
compressed = zlib.compress(raw, 9)
ratio = len(compressed) / len(raw) if raw else 0
complexity = 'random' if ratio > 0.95 else 'complex' if ratio > 0.5 else 'structured' if ratio > 0.1 else 'trivial'
return json.dumps({
    'raw_size': len(raw),
    'compressed_size': len(compressed),
    'ratio': round(ratio, 4),
    'classification': complexity
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Lempel-Ziv complexity: count of distinct patterns in binary string
CREATE OR REPLACE FUNCTION substrate.lz_complexity(binary_str TEXT)
RETURNS INT AS $$
s = binary_str
n = len(s)
if n == 0: return 0
c = 1; l = 1; i = 0; k = 1; k_max = 1
while True:
    if s[i + k - 1] == s[l + k - 1] if (l + k - 1) < n else False:
        k += 1
        if l + k > n:
            c += 1
            break
    else:
        k_max = max(k, k_max)
        i += 1
        if i == l:
            c += 1
            l += k_max
            if l >= n: break
            i = 0; k = 1; k_max = 1
        else:
            k = 1
return c
$$ LANGUAGE plpython3u IMMUTABLE;

-- Approximate entropy (ApEn) — regularity measure for time series
CREATE OR REPLACE FUNCTION substrate.approx_entropy(data FLOAT8[], m INT DEFAULT 2, r_factor FLOAT8 DEFAULT 0.2)
RETURNS FLOAT8 AS $$
import math
n = len(data)
if n < m + 1: return 0
r = r_factor * (max(data) - min(data)) / (max(1, max(data) - min(data)) if max(data) != min(data) else 1)
if r == 0: r = 0.2
def phi(dim):
    templates = [data[i:i+dim] for i in range(n - dim + 1)]
    counts = []
    for i, t in enumerate(templates):
        count = sum(1 for j, u in enumerate(templates) if all(abs(t[k]-u[k]) <= r for k in range(dim)))
        counts.append(count / len(templates))
    return sum(math.log(c) for c in counts if c > 0) / len(counts)
return phi(m) - phi(m + 1)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== PHASE TRANSITIONS =====

-- Order parameter: fraction of elements in majority state
CREATE OR REPLACE FUNCTION substrate.order_parameter(states INT[])
RETURNS FLOAT8 AS $$
from collections import Counter
if not states: return 0
counts = Counter(states)
return counts.most_common(1)[0][1] / len(states)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Susceptibility: variance of order parameter (peak = critical point)
CREATE OR REPLACE FUNCTION substrate.susceptibility(order_params FLOAT8[])
RETURNS FLOAT8 AS $$
n = len(order_params)
if n < 2: return 0
mu = sum(order_params) / n
return sum((x - mu)**2 for x in order_params) / n
$$ LANGUAGE plpython3u IMMUTABLE;

-- Correlation length estimate: how far correlations extend
CREATE OR REPLACE FUNCTION substrate.correlation_length(autocorr FLOAT8[], threshold FLOAT8 DEFAULT 0.368)
RETURNS INT AS $$
for i, c in enumerate(autocorr):
    if c < threshold:
        return i
return len(autocorr)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Self-organized criticality indicator: is the system at a critical point?
CREATE OR REPLACE FUNCTION substrate.soc_indicator(
    event_sizes FLOAT8[], power_law_alpha FLOAT8 DEFAULT 0,
    order_param FLOAT8 DEFAULT 0, susceptibility FLOAT8 DEFAULT 0
)
RETURNS JSONB AS $$
import json, math
from collections import Counter
# Check for power law in event sizes
if not power_law_alpha and event_sizes:
    filtered = [x for x in event_sizes if x > 0]
    if len(filtered) > 5:
        xmin = min(filtered)
        n = len(filtered)
        power_law_alpha = 1 + n / sum(math.log(x / xmin) for x in filtered)
is_power_law = 1.5 < power_law_alpha < 3.5 if power_law_alpha else False
is_critical = is_power_law and susceptibility > 0.1
return json.dumps({
    'alpha': round(power_law_alpha, 3) if power_law_alpha else None,
    'is_power_law': is_power_law,
    'order_parameter': round(order_param, 4),
    'susceptibility': round(susceptibility, 4),
    'likely_critical': is_critical,
    'classification': 'critical' if is_critical else 'subcritical' if order_param > 0.8 else 'supercritical' if order_param < 0.2 else 'transition'
})
$$ LANGUAGE plpython3u IMMUTABLE;
