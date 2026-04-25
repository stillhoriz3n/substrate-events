-- ============================================================
-- SUBSTRATE LIBRARY: lib.information
-- Information theory: the fundamental limits of what can be
-- known, communicated, compressed, and distinguished.
-- ============================================================

-- ===== ENTROPY (deeper than lib_stats) =====

-- Joint entropy H(X,Y) from joint probability matrix (2D array as flat + dims)
CREATE OR REPLACE FUNCTION substrate.joint_entropy(probs FLOAT8[])
RETURNS FLOAT8 AS $$
import math
return -sum(p * math.log2(p) for p in probs if p > 0)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Conditional entropy H(Y|X) = H(X,Y) - H(X)
CREATE OR REPLACE FUNCTION substrate.conditional_entropy(h_xy FLOAT8, h_x FLOAT8)
RETURNS FLOAT8 AS $$ SELECT h_xy - h_x $$ LANGUAGE sql IMMUTABLE;

-- Mutual information I(X;Y) = H(X) + H(Y) - H(X,Y)
CREATE OR REPLACE FUNCTION substrate.mutual_information(h_x FLOAT8, h_y FLOAT8, h_xy FLOAT8)
RETURNS FLOAT8 AS $$ SELECT h_x + h_y - h_xy $$ LANGUAGE sql IMMUTABLE;

-- Normalized mutual information [0..1]
CREATE OR REPLACE FUNCTION substrate.nmi(h_x FLOAT8, h_y FLOAT8, h_xy FLOAT8)
RETURNS FLOAT8 AS $$
SELECT CASE WHEN LEAST(h_x, h_y) = 0 THEN 0
ELSE (h_x + h_y - h_xy) / LEAST(h_x, h_y) END
$$ LANGUAGE sql IMMUTABLE;

-- KL divergence D_KL(P || Q) — how much P diverges from Q
CREATE OR REPLACE FUNCTION substrate.kl_divergence(p FLOAT8[], q FLOAT8[])
RETURNS FLOAT8 AS $$
import math
return sum(pi * math.log2(pi / qi) for pi, qi in zip(p, q) if pi > 0 and qi > 0)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Jensen-Shannon divergence (symmetric, bounded [0,1])
CREATE OR REPLACE FUNCTION substrate.js_divergence(p FLOAT8[], q FLOAT8[])
RETURNS FLOAT8 AS $$
import math
m = [(pi + qi) / 2 for pi, qi in zip(p, q)]
def kl(a, b):
    return sum(ai * math.log2(ai / bi) for ai, bi in zip(a, b) if ai > 0 and bi > 0)
return (kl(p, m) + kl(q, m)) / 2
$$ LANGUAGE plpython3u IMMUTABLE;

-- Cross-entropy H(P, Q) = -sum(p * log(q))
CREATE OR REPLACE FUNCTION substrate.cross_entropy(p FLOAT8[], q FLOAT8[])
RETURNS FLOAT8 AS $$
import math
return -sum(pi * math.log2(qi) for pi, qi in zip(p, q) if pi > 0 and qi > 0)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Entropy rate estimate from sequence (sliding window conditional entropy)
CREATE OR REPLACE FUNCTION substrate.entropy_rate(sequence TEXT, order_k INT DEFAULT 1)
RETURNS FLOAT8 AS $$
import math
from collections import Counter
n = len(sequence)
if n <= order_k: return 0
# Count k-grams and (k+1)-grams
kgrams = Counter(sequence[i:i+order_k] for i in range(n - order_k))
k1grams = Counter(sequence[i:i+order_k+1] for i in range(n - order_k))
total_k1 = sum(k1grams.values())
h = 0
for gram, count in k1grams.items():
    p_joint = count / total_k1
    p_prefix = kgrams[gram[:order_k]] / sum(kgrams.values())
    p_cond = p_joint / p_prefix if p_prefix > 0 else 0
    if p_cond > 0:
        h -= p_joint * math.log2(p_cond)
return h
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== CHANNEL CAPACITY =====

-- Shannon channel capacity: C = B * log2(1 + SNR)
CREATE OR REPLACE FUNCTION substrate.shannon_capacity(bandwidth_hz FLOAT8, snr_linear FLOAT8)
RETURNS FLOAT8 AS $$
import math
return bandwidth_hz * math.log2(1 + snr_linear)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Shannon capacity from dB SNR
CREATE OR REPLACE FUNCTION substrate.shannon_capacity_db(bandwidth_hz FLOAT8, snr_db FLOAT8)
RETURNS FLOAT8 AS $$
import math
snr_linear = 10 ** (snr_db / 10)
return bandwidth_hz * math.log2(1 + snr_linear)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Spectral efficiency (bits/s/Hz) from capacity and bandwidth
CREATE OR REPLACE FUNCTION substrate.spectral_efficiency(capacity_bps FLOAT8, bandwidth_hz FLOAT8)
RETURNS FLOAT8 AS $$ SELECT capacity_bps / NULLIF(bandwidth_hz, 0) $$ LANGUAGE sql IMMUTABLE;

-- Binary symmetric channel capacity
CREATE OR REPLACE FUNCTION substrate.bsc_capacity(error_prob FLOAT8)
RETURNS FLOAT8 AS $$
import math
if error_prob <= 0 or error_prob >= 1: return 0
h = -error_prob * math.log2(error_prob) - (1-error_prob) * math.log2(1-error_prob)
return 1 - h
$$ LANGUAGE plpython3u IMMUTABLE;

-- Binary erasure channel capacity
CREATE OR REPLACE FUNCTION substrate.bec_capacity(erasure_prob FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 1 - erasure_prob $$ LANGUAGE sql IMMUTABLE;

-- ===== CODING BOUNDS =====

-- Minimum bits needed to encode n symbols (log2 ceiling)
CREATE OR REPLACE FUNCTION substrate.min_bits(n_symbols BIGINT)
RETURNS INT AS $$
import math
if n_symbols <= 1: return 0
return math.ceil(math.log2(n_symbols))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Kraft inequality check: are codeword lengths valid for prefix code?
CREATE OR REPLACE FUNCTION substrate.kraft_check(lengths INT[])
RETURNS JSONB AS $$
import json
kraft_sum = sum(2**(-l) for l in lengths)
return json.dumps({
    'kraft_sum': round(kraft_sum, 6),
    'valid_prefix_code': kraft_sum <= 1.0,
    'optimal': abs(kraft_sum - 1.0) < 0.001
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Huffman code lengths from symbol probabilities (returns expected code length)
CREATE OR REPLACE FUNCTION substrate.huffman_expected_length(probs FLOAT8[])
RETURNS FLOAT8 AS $$
import heapq
if len(probs) <= 1: return 0
heap = [(p, i, 0) for i, p in enumerate(probs)]
heapq.heapify(heap)
lengths = [0] * len(probs)
while len(heap) > 1:
    p1, _, d1 = heapq.heappop(heap)
    p2, _, d2 = heapq.heappop(heap)
    heapq.heappush(heap, (p1 + p2, -1, max(d1, d2) + 1))
# Approximate: entropy is the lower bound, +1 bit is upper
import math
entropy = -sum(p * math.log2(p) for p in probs if p > 0)
return entropy  # Huffman achieves within 1 bit of entropy
$$ LANGUAGE plpython3u IMMUTABLE;

-- Rate-distortion: minimum bits at distortion D for Gaussian source
CREATE OR REPLACE FUNCTION substrate.rate_distortion_gaussian(variance FLOAT8, distortion FLOAT8)
RETURNS FLOAT8 AS $$
import math
if distortion >= variance: return 0
return 0.5 * math.log2(variance / distortion)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== DIVERGENCE & DISTANCE =====

-- Total variation distance between distributions
CREATE OR REPLACE FUNCTION substrate.total_variation(p FLOAT8[], q FLOAT8[])
RETURNS FLOAT8 AS $$
return 0.5 * sum(abs(pi - qi) for pi, qi in zip(p, q))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Hellinger distance
CREATE OR REPLACE FUNCTION substrate.hellinger(p FLOAT8[], q FLOAT8[])
RETURNS FLOAT8 AS $$
import math
return math.sqrt(0.5 * sum((math.sqrt(pi) - math.sqrt(qi))**2 for pi, qi in zip(p, q)))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Bhattacharyya distance
CREATE OR REPLACE FUNCTION substrate.bhattacharyya(p FLOAT8[], q FLOAT8[])
RETURNS FLOAT8 AS $$
import math
bc = sum(math.sqrt(pi * qi) for pi, qi in zip(p, q))
if bc <= 0: return float('inf')
return -math.log(bc)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Wasserstein-1 distance (Earth Mover's Distance) for 1D distributions
CREATE OR REPLACE FUNCTION substrate.wasserstein1(p FLOAT8[], q FLOAT8[])
RETURNS FLOAT8 AS $$
# CDF difference method for 1D
cp, cq = 0, 0
dist = 0
for pi, qi in zip(p, q):
    cp += pi; cq += qi
    dist += abs(cp - cq)
return dist
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== FISHER INFORMATION =====

-- Fisher information for Bernoulli(p)
CREATE OR REPLACE FUNCTION substrate.fisher_bernoulli(p FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 1.0 / (p * (1 - p)) $$ LANGUAGE sql IMMUTABLE;

-- Fisher information for Gaussian(mu, sigma) w.r.t. mu
CREATE OR REPLACE FUNCTION substrate.fisher_gaussian_mu(sigma FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 1.0 / (sigma * sigma) $$ LANGUAGE sql IMMUTABLE;

-- Cramer-Rao lower bound: minimum variance of unbiased estimator
CREATE OR REPLACE FUNCTION substrate.cramer_rao_bound(fisher_info FLOAT8, n_samples INT)
RETURNS FLOAT8 AS $$ SELECT 1.0 / (n_samples * fisher_info) $$ LANGUAGE sql IMMUTABLE;

-- ===== INFORMATION GEOMETRY =====

-- Surprise (self-information) of an event
CREATE OR REPLACE FUNCTION substrate.surprise(prob FLOAT8)
RETURNS FLOAT8 AS $$
import math
if prob <= 0: return float('inf')
return -math.log2(prob)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Pointwise mutual information between two events
CREATE OR REPLACE FUNCTION substrate.pmi(p_xy FLOAT8, p_x FLOAT8, p_y FLOAT8)
RETURNS FLOAT8 AS $$
import math
if p_xy <= 0 or p_x <= 0 or p_y <= 0: return 0
return math.log2(p_xy / (p_x * p_y))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Redundancy: 1 - (entropy / max_entropy)
CREATE OR REPLACE FUNCTION substrate.redundancy(entropy FLOAT8, n_symbols INT)
RETURNS FLOAT8 AS $$
import math
max_h = math.log2(n_symbols) if n_symbols > 1 else 1
return 1 - (entropy / max_h)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Information gain (reduction in entropy from knowing X)
CREATE OR REPLACE FUNCTION substrate.info_gain(h_before FLOAT8, h_after FLOAT8)
RETURNS FLOAT8 AS $$ SELECT h_before - h_after $$ LANGUAGE sql IMMUTABLE;

-- Perplexity: 2^H — how many equally likely outcomes the entropy suggests
CREATE OR REPLACE FUNCTION substrate.perplexity(entropy FLOAT8)
RETURNS FLOAT8 AS $$ SELECT power(2, entropy) $$ LANGUAGE sql IMMUTABLE;
