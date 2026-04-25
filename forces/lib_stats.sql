-- ============================================================
-- SUBSTRATE LIBRARY: lib.stats
-- Statistical functions, distributions, and estimators
-- ============================================================

-- Descriptive stats from array
CREATE OR REPLACE FUNCTION substrate.describe(arr FLOAT8[])
RETURNS TABLE(n INT, mean FLOAT8, median FLOAT8, stddev FLOAT8, min FLOAT8, max FLOAT8, p25 FLOAT8, p75 FLOAT8, iqr FLOAT8) AS $$
import statistics
a = sorted(arr)
n = len(a)
if n == 0:
    return [(0, None, None, None, None, None, None, None, None)]
mu = statistics.mean(a)
med = statistics.median(a)
sd = statistics.stdev(a) if n > 1 else 0
q1 = a[n//4] if n >= 4 else a[0]
q3 = a[(3*n)//4] if n >= 4 else a[-1]
return [(n, mu, med, sd, a[0], a[-1], q1, q3, q3-q1)]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Percentile from array
CREATE OR REPLACE FUNCTION substrate.percentile(arr FLOAT8[], p FLOAT8)
RETURNS FLOAT8 AS $$
a = sorted(arr)
n = len(a)
if n == 0: return None
k = (n - 1) * p / 100.0
f = int(k)
c = f + 1 if f + 1 < n else f
return a[f] + (k - f) * (a[c] - a[f])
$$ LANGUAGE plpython3u IMMUTABLE;

-- Exponential moving average
CREATE OR REPLACE FUNCTION substrate.ema(arr FLOAT8[], alpha FLOAT8 DEFAULT 0.3)
RETURNS FLOAT8[] AS $$
if not arr: return []
result = [arr[0]]
for v in arr[1:]:
    result.append(alpha * v + (1 - alpha) * result[-1])
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- Simple moving average
CREATE OR REPLACE FUNCTION substrate.sma(arr FLOAT8[], win_size INT DEFAULT 5)
RETURNS FLOAT8[] AS $$
if not arr or win_size < 1: return []
result = []
for i in range(len(arr)):
    start = max(0, i - win_size + 1)
    result.append(sum(arr[start:i+1]) / (i - start + 1))
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- Z-score
CREATE OR REPLACE FUNCTION substrate.zscore(val FLOAT8, mean FLOAT8, stddev FLOAT8)
RETURNS FLOAT8 AS $$ SELECT CASE WHEN stddev = 0 THEN 0 ELSE (val - mean) / stddev END $$ LANGUAGE sql IMMUTABLE;

-- Normalize array to [0,1]
CREATE OR REPLACE FUNCTION substrate.normalize(arr FLOAT8[])
RETURNS FLOAT8[] AS $$
mn, mx = min(arr), max(arr)
rng = mx - mn
if rng == 0: return [0.5] * len(arr)
return [(v - mn) / rng for v in arr]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Pearson correlation
CREATE OR REPLACE FUNCTION substrate.pearson(x FLOAT8[], y FLOAT8[])
RETURNS FLOAT8 AS $$
import statistics
n = min(len(x), len(y))
if n < 2: return None
return statistics.correlation(x[:n], y[:n])
$$ LANGUAGE plpython3u IMMUTABLE;

-- Covariance
CREATE OR REPLACE FUNCTION substrate.covariance(x FLOAT8[], y FLOAT8[])
RETURNS FLOAT8 AS $$
import statistics
n = min(len(x), len(y))
if n < 2: return None
return statistics.covariance(x[:n], y[:n])
$$ LANGUAGE plpython3u IMMUTABLE;

-- Linear regression: returns [slope, intercept, r_squared]
CREATE OR REPLACE FUNCTION substrate.linreg(x FLOAT8[], y FLOAT8[])
RETURNS FLOAT8[] AS $$
n = min(len(x), len(y))
if n < 2: return [None, None, None]
sx = sum(x[:n]); sy = sum(y[:n])
sxx = sum(a*a for a in x[:n]); sxy = sum(a*b for a,b in zip(x[:n],y[:n]))
syy = sum(b*b for b in y[:n])
denom = n*sxx - sx*sx
if denom == 0: return [None, None, None]
slope = (n*sxy - sx*sy) / denom
intercept = (sy - slope*sx) / n
ss_res = sum((y[i] - (slope*x[i]+intercept))**2 for i in range(n))
ss_tot = syy - sy*sy/n
r2 = 1 - ss_res/ss_tot if ss_tot != 0 else 1
return [slope, intercept, r2]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Bayesian update: posterior odds given prior odds and likelihood ratio
CREATE OR REPLACE FUNCTION substrate.bayes_update(prior_prob FLOAT8, likelihood_ratio FLOAT8)
RETURNS FLOAT8 AS $$
SELECT CASE
    WHEN prior_prob >= 1 THEN 1.0
    WHEN prior_prob <= 0 THEN 0.0
    ELSE (prior_prob * likelihood_ratio) / (prior_prob * likelihood_ratio + (1 - prior_prob))
END
$$ LANGUAGE sql IMMUTABLE;

-- Shannon entropy of a probability distribution (array sums to 1)
CREATE OR REPLACE FUNCTION substrate.shannon_entropy(probs FLOAT8[])
RETURNS FLOAT8 AS $$
import math
return -sum(p * math.log2(p) for p in probs if p > 0)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Entropy of raw data (bytes) — bits per byte
CREATE OR REPLACE FUNCTION substrate.data_entropy(data BYTEA)
RETURNS FLOAT8 AS $$
import math
if not data: return 0
counts = [0]*256
for b in data:
    counts[b] += 1
n = len(data)
return -sum((c/n)*math.log2(c/n) for c in counts if c > 0)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Poisson probability P(X=k) given lambda
CREATE OR REPLACE FUNCTION substrate.poisson_pmf(k INT, lam FLOAT8)
RETURNS FLOAT8 AS $$
import math
return (lam**k * math.exp(-lam)) / math.factorial(k)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Normal CDF (probability X <= x)
CREATE OR REPLACE FUNCTION substrate.normal_cdf(x FLOAT8, mu FLOAT8 DEFAULT 0, sigma FLOAT8 DEFAULT 1)
RETURNS FLOAT8 AS $$
import math
return 0.5 * (1 + math.erf((x - mu) / (sigma * math.sqrt(2))))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Normal PDF
CREATE OR REPLACE FUNCTION substrate.normal_pdf(x FLOAT8, mu FLOAT8 DEFAULT 0, sigma FLOAT8 DEFAULT 1)
RETURNS FLOAT8 AS $$
import math
return math.exp(-0.5*((x-mu)/sigma)**2) / (sigma * math.sqrt(2*math.pi))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Binomial probability P(X=k)
CREATE OR REPLACE FUNCTION substrate.binomial_pmf(k INT, n INT, p FLOAT8)
RETURNS FLOAT8 AS $$
import math
return math.comb(n, k) * p**k * (1-p)**(n-k)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Chi-squared test statistic from observed and expected arrays
CREATE OR REPLACE FUNCTION substrate.chi_squared(observed FLOAT8[], expected FLOAT8[])
RETURNS FLOAT8 AS $$
return sum((o-e)**2/e for o,e in zip(observed, expected) if e > 0)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Welford's online mean and variance: process array, return [mean, variance, count]
CREATE OR REPLACE FUNCTION substrate.welford(arr FLOAT8[])
RETURNS FLOAT8[] AS $$
n = 0; mean = 0; m2 = 0
for x in arr:
    n += 1
    delta = x - mean
    mean += delta / n
    m2 += delta * (x - mean)
var = m2 / n if n > 0 else 0
return [mean, var, float(n)]
$$ LANGUAGE plpython3u IMMUTABLE;
