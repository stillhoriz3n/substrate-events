-- ============================================================
-- SUBSTRATE LIBRARY: lib.math
-- Core mathematical functions and constants
-- ============================================================

-- Constants as zero-cost functions
CREATE OR REPLACE FUNCTION substrate.phi() RETURNS FLOAT8 AS $$ SELECT 1.6180339887498948482::float8 $$ LANGUAGE sql IMMUTABLE;
CREATE OR REPLACE FUNCTION substrate.euler() RETURNS FLOAT8 AS $$ SELECT 2.7182818284590452354::float8 $$ LANGUAGE sql IMMUTABLE;
CREATE OR REPLACE FUNCTION substrate.tau() RETURNS FLOAT8 AS $$ SELECT 6.2831853071795864769::float8 $$ LANGUAGE sql IMMUTABLE;
CREATE OR REPLACE FUNCTION substrate.sqrt2() RETURNS FLOAT8 AS $$ SELECT 1.4142135623730950488::float8 $$ LANGUAGE sql IMMUTABLE;
CREATE OR REPLACE FUNCTION substrate.planck() RETURNS FLOAT8 AS $$ SELECT 6.62607015e-34::float8 $$ LANGUAGE sql IMMUTABLE;
CREATE OR REPLACE FUNCTION substrate.boltzmann() RETURNS FLOAT8 AS $$ SELECT 1.380649e-23::float8 $$ LANGUAGE sql IMMUTABLE;
CREATE OR REPLACE FUNCTION substrate.avogadro() RETURNS FLOAT8 AS $$ SELECT 6.02214076e23::float8 $$ LANGUAGE sql IMMUTABLE;
CREATE OR REPLACE FUNCTION substrate.speed_of_light() RETURNS FLOAT8 AS $$ SELECT 299792458.0::float8 $$ LANGUAGE sql IMMUTABLE;

-- Clamp value to range
CREATE OR REPLACE FUNCTION substrate.clamp(val FLOAT8, lo FLOAT8, hi FLOAT8)
RETURNS FLOAT8 AS $$ SELECT GREATEST(lo, LEAST(hi, val)) $$ LANGUAGE sql IMMUTABLE;

-- Linear interpolation
CREATE OR REPLACE FUNCTION substrate.lerp(a FLOAT8, b FLOAT8, t FLOAT8)
RETURNS FLOAT8 AS $$ SELECT a + (b - a) * t $$ LANGUAGE sql IMMUTABLE;

-- Inverse lerp: given value in [a,b], return t in [0,1]
CREATE OR REPLACE FUNCTION substrate.inv_lerp(a FLOAT8, b FLOAT8, val FLOAT8)
RETURNS FLOAT8 AS $$ SELECT CASE WHEN b = a THEN 0 ELSE (val - a) / (b - a) END $$ LANGUAGE sql IMMUTABLE;

-- Remap value from one range to another
CREATE OR REPLACE FUNCTION substrate.remap(val FLOAT8, in_lo FLOAT8, in_hi FLOAT8, out_lo FLOAT8, out_hi FLOAT8)
RETURNS FLOAT8 AS $$ SELECT out_lo + (out_hi - out_lo) * ((val - in_lo) / NULLIF(in_hi - in_lo, 0)) $$ LANGUAGE sql IMMUTABLE;

-- Sigmoid / logistic function
CREATE OR REPLACE FUNCTION substrate.sigmoid(x FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 1.0 / (1.0 + exp(-x)) $$ LANGUAGE sql IMMUTABLE;

-- Softmax for an array
CREATE OR REPLACE FUNCTION substrate.softmax(arr FLOAT8[])
RETURNS FLOAT8[] AS $$
import math
mx = max(arr)
exps = [math.exp(x - mx) for x in arr]
s = sum(exps)
return [e / s for e in exps]
$$ LANGUAGE plpython3u IMMUTABLE;

-- ReLU
CREATE OR REPLACE FUNCTION substrate.relu(x FLOAT8)
RETURNS FLOAT8 AS $$ SELECT GREATEST(0.0, x) $$ LANGUAGE sql IMMUTABLE;

-- Factorial (iterative, up to 170)
CREATE OR REPLACE FUNCTION substrate.factorial(n INT)
RETURNS FLOAT8 AS $$
import math
return float(math.factorial(n))
$$ LANGUAGE plpython3u IMMUTABLE;

-- nCr — combinations
CREATE OR REPLACE FUNCTION substrate.comb(n INT, k INT)
RETURNS FLOAT8 AS $$
import math
return float(math.comb(n, k))
$$ LANGUAGE plpython3u IMMUTABLE;

-- nPr — permutations
CREATE OR REPLACE FUNCTION substrate.perm(n INT, k INT)
RETURNS FLOAT8 AS $$
import math
return float(math.perm(n, k))
$$ LANGUAGE plpython3u IMMUTABLE;

-- GCD and LCM
CREATE OR REPLACE FUNCTION substrate.gcd_val(a BIGINT, b BIGINT)
RETURNS BIGINT AS $$
import math
return math.gcd(a, b)
$$ LANGUAGE plpython3u IMMUTABLE;

CREATE OR REPLACE FUNCTION substrate.lcm_val(a BIGINT, b BIGINT)
RETURNS BIGINT AS $$
import math
return math.lcm(a, b)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Is prime (deterministic Miller-Rabin for < 3.3e24)
CREATE OR REPLACE FUNCTION substrate.is_prime(n BIGINT)
RETURNS BOOLEAN AS $$
if n < 2: return False
if n < 4: return True
if n % 2 == 0 or n % 3 == 0: return False
d, r = n - 1, 0
while d % 2 == 0:
    d //= 2
    r += 1
for a in [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37]:
    if a >= n: continue
    x = pow(a, d, n)
    if x == 1 or x == n - 1: continue
    for _ in range(r - 1):
        x = pow(x, 2, n)
        if x == n - 1: break
    else:
        return False
return True
$$ LANGUAGE plpython3u IMMUTABLE;

-- Fibonacci (closed-form Binet, exact for n < 72)
CREATE OR REPLACE FUNCTION substrate.fib(n INT)
RETURNS BIGINT AS $$
import math
phi = (1 + math.sqrt(5)) / 2
return round(phi**n / math.sqrt(5))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Euclidean distance (2D)
CREATE OR REPLACE FUNCTION substrate.dist2d(x1 FLOAT8, y1 FLOAT8, x2 FLOAT8, y2 FLOAT8)
RETURNS FLOAT8 AS $$ SELECT sqrt((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1)) $$ LANGUAGE sql IMMUTABLE;

-- Manhattan distance (2D)
CREATE OR REPLACE FUNCTION substrate.manhattan2d(x1 FLOAT8, y1 FLOAT8, x2 FLOAT8, y2 FLOAT8)
RETURNS FLOAT8 AS $$ SELECT abs(x2-x1) + abs(y2-y1) $$ LANGUAGE sql IMMUTABLE;

-- Haversine distance (lat/lon in degrees → meters)
CREATE OR REPLACE FUNCTION substrate.haversine(lat1 FLOAT8, lon1 FLOAT8, lat2 FLOAT8, lon2 FLOAT8)
RETURNS FLOAT8 AS $$
import math
R = 6371000
rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
dlat = math.radians(lat2 - lat1)
dlon = math.radians(lon2 - lon1)
a = math.sin(dlat/2)**2 + math.cos(rlat1)*math.cos(rlat2)*math.sin(dlon/2)**2
return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Quadratic formula: returns array of [x1, x2] or empty if no real roots
CREATE OR REPLACE FUNCTION substrate.quadratic(a FLOAT8, b FLOAT8, c FLOAT8)
RETURNS FLOAT8[] AS $$
import math
disc = b*b - 4*a*c
if disc < 0: return []
sq = math.sqrt(disc)
return [(-b + sq)/(2*a), (-b - sq)/(2*a)]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Newton's method root finder: f(x)=0 given f and f' as SQL expressions
-- (simplified: finds root of polynomial a*x^2 + b*x + c near guess)
CREATE OR REPLACE FUNCTION substrate.newton_poly(a FLOAT8, b FLOAT8, c FLOAT8, guess FLOAT8 DEFAULT 0, iters INT DEFAULT 50)
RETURNS FLOAT8 AS $$
x = guess
for _ in range(iters):
    fx = a*x*x + b*x + c
    fpx = 2*a*x + b
    if abs(fpx) < 1e-15: break
    x = x - fx/fpx
return x
$$ LANGUAGE plpython3u IMMUTABLE;

-- Numerical integration (Simpson's rule) of polynomial a*x^2 + b*x + c over [lo, hi]
CREATE OR REPLACE FUNCTION substrate.integrate_poly(a FLOAT8, b FLOAT8, c FLOAT8, lo FLOAT8, hi FLOAT8)
RETURNS FLOAT8 AS $$
SELECT (a/3.0)*(hi*hi*hi - lo*lo*lo) + (b/2.0)*(hi*hi - lo*lo) + c*(hi - lo)
$$ LANGUAGE sql IMMUTABLE;

-- Derivative of polynomial at point: f'(x) for a*x^2 + b*x + c
CREATE OR REPLACE FUNCTION substrate.deriv_poly(a FLOAT8, b FLOAT8, c FLOAT8, x FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 2*a*x + b $$ LANGUAGE sql IMMUTABLE;

-- Log in any base
CREATE OR REPLACE FUNCTION substrate.logb(val FLOAT8, base FLOAT8)
RETURNS FLOAT8 AS $$ SELECT ln(val) / ln(base) $$ LANGUAGE sql IMMUTABLE;
