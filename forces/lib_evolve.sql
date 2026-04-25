-- ============================================================
-- SUBSTRATE LIBRARY: lib.evolve
-- Adaptation, selection, fitness landscapes, population dynamics.
-- How the substrate learns and changes.
-- ============================================================

-- ===== FITNESS & SELECTION =====

-- Fitness-proportionate selection probability
CREATE OR REPLACE FUNCTION substrate.fitness_proportion(fitness FLOAT8[], idx INT)
RETURNS FLOAT8 AS $$
total = sum(fitness)
if total == 0: return 1.0 / len(fitness)
return fitness[idx] / total
$$ LANGUAGE plpython3u IMMUTABLE;

-- Tournament selection: pick best of k random from population
CREATE OR REPLACE FUNCTION substrate.tournament_select(fitness FLOAT8[], k INT DEFAULT 3)
RETURNS INT AS $$
import random
contestants = random.sample(range(len(fitness)), min(k, len(fitness)))
return max(contestants, key=lambda i: fitness[i])
$$ LANGUAGE plpython3u;

-- Roulette wheel selection
CREATE OR REPLACE FUNCTION substrate.roulette_select(fitness FLOAT8[])
RETURNS INT AS $$
import random
total = sum(fitness)
if total == 0: return random.randint(0, len(fitness) - 1)
r = random.uniform(0, total)
running = 0
for i, f in enumerate(fitness):
    running += f
    if running >= r: return i
return len(fitness) - 1
$$ LANGUAGE plpython3u;

-- Boltzmann selection probability: exp(fitness/T) / sum(exp(fitness/T))
CREATE OR REPLACE FUNCTION substrate.boltzmann_prob(fitness FLOAT8[], temperature FLOAT8 DEFAULT 1.0)
RETURNS FLOAT8[] AS $$
import math
max_f = max(fitness)
exps = [math.exp((f - max_f) / temperature) for f in fitness]
total = sum(exps)
return [e / total for e in exps]
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== GENETIC OPERATORS =====

-- Single-point crossover of two binary strings
CREATE OR REPLACE FUNCTION substrate.crossover_1pt(parent_a TEXT, parent_b TEXT)
RETURNS TEXT[] AS $$
import random
point = random.randint(1, min(len(parent_a), len(parent_b)) - 1)
child_a = parent_a[:point] + parent_b[point:]
child_b = parent_b[:point] + parent_a[point:]
return [child_a, child_b]
$$ LANGUAGE plpython3u;

-- Uniform crossover
CREATE OR REPLACE FUNCTION substrate.crossover_uniform(parent_a TEXT, parent_b TEXT, mix_rate FLOAT8 DEFAULT 0.5)
RETURNS TEXT AS $$
import random
return ''.join(a if random.random() > mix_rate else b for a, b in zip(parent_a, parent_b))
$$ LANGUAGE plpython3u;

-- Bit-flip mutation
CREATE OR REPLACE FUNCTION substrate.mutate_bitflip(genome TEXT, rate FLOAT8 DEFAULT 0.01)
RETURNS TEXT AS $$
import random
result = list(genome)
for i in range(len(result)):
    if random.random() < rate:
        result[i] = '0' if result[i] == '1' else '1'
return ''.join(result)
$$ LANGUAGE plpython3u;

-- Gaussian mutation for real-valued genes
CREATE OR REPLACE FUNCTION substrate.mutate_gaussian(genes FLOAT8[], sigma FLOAT8 DEFAULT 0.1, rate FLOAT8 DEFAULT 0.1)
RETURNS FLOAT8[] AS $$
import random
return [g + random.gauss(0, sigma) if random.random() < rate else g for g in genes]
$$ LANGUAGE plpython3u;

-- ===== MULTI-ARMED BANDIT =====

-- UCB1 (Upper Confidence Bound): select arm with highest UCB
CREATE OR REPLACE FUNCTION substrate.ucb1(
    n_pulls INT[], total_rewards FLOAT8[], total_pulls INT
)
RETURNS INT AS $$
import math
n = len(n_pulls)
for i in range(n):
    if n_pulls[i] == 0: return i  # try unexplored
ucb = [total_rewards[i]/n_pulls[i] + math.sqrt(2*math.log(total_pulls)/n_pulls[i]) for i in range(n)]
return max(range(n), key=lambda i: ucb[i])
$$ LANGUAGE plpython3u IMMUTABLE;

-- Thompson sampling: select arm by sampling from posterior Beta distributions
CREATE OR REPLACE FUNCTION substrate.thompson_select(successes INT[], failures INT[])
RETURNS INT AS $$
import random
samples = [random.betavariate(s + 1, f + 1) for s, f in zip(successes, failures)]
return max(range(len(samples)), key=lambda i: samples[i])
$$ LANGUAGE plpython3u;

-- Epsilon-greedy: exploit best arm with prob (1-eps), explore random with eps
CREATE OR REPLACE FUNCTION substrate.epsilon_greedy(rewards FLOAT8[], n_pulls INT[], epsilon FLOAT8 DEFAULT 0.1)
RETURNS INT AS $$
import random
if random.random() < epsilon:
    return random.randint(0, len(rewards) - 1)
avgs = [rewards[i]/max(1,n_pulls[i]) for i in range(len(rewards))]
return max(range(len(avgs)), key=lambda i: avgs[i])
$$ LANGUAGE plpython3u;

-- ===== POPULATION DYNAMICS =====

-- Lotka-Volterra step: predator-prey dynamics
CREATE OR REPLACE FUNCTION substrate.lotka_volterra_step(
    prey FLOAT8, predator FLOAT8, dt FLOAT8 DEFAULT 0.01,
    alpha FLOAT8 DEFAULT 1.1, beta FLOAT8 DEFAULT 0.4,
    gamma FLOAT8 DEFAULT 0.4, delta FLOAT8 DEFAULT 0.1
)
RETURNS FLOAT8[] AS $$
dprey = (alpha * prey - beta * prey * predator) * dt
dpred = (delta * prey * predator - gamma * predator) * dt
return [max(0, prey + dprey), max(0, predator + dpred)]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Logistic growth: dN/dt = r*N*(1 - N/K)
CREATE OR REPLACE FUNCTION substrate.logistic_growth(
    population FLOAT8, growth_rate FLOAT8, carrying_capacity FLOAT8, dt FLOAT8 DEFAULT 1.0
)
RETURNS FLOAT8 AS $$
SELECT population + growth_rate * population * (1 - population / carrying_capacity) * dt
$$ LANGUAGE sql IMMUTABLE;

-- Replicator dynamics: x_i' = x_i * (f_i - avg_f)
CREATE OR REPLACE FUNCTION substrate.replicator_step(frequencies FLOAT8[], fitness FLOAT8[], dt FLOAT8 DEFAULT 0.01)
RETURNS FLOAT8[] AS $$
avg_f = sum(x * f for x, f in zip(frequencies, fitness))
new_freq = [x + x * (f - avg_f) * dt for x, f in zip(frequencies, fitness)]
total = sum(new_freq)
return [max(0, x / total) for x in new_freq] if total > 0 else frequencies
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== RATING SYSTEMS =====

-- ELO rating update
CREATE OR REPLACE FUNCTION substrate.elo_update(
    rating_a FLOAT8, rating_b FLOAT8, score_a FLOAT8,
    k_factor FLOAT8 DEFAULT 32
)
RETURNS FLOAT8[] AS $$
import math
expected_a = 1 / (1 + 10**((rating_b - rating_a) / 400))
new_a = rating_a + k_factor * (score_a - expected_a)
new_b = rating_b + k_factor * ((1 - score_a) - (1 - expected_a))
return [round(new_a, 1), round(new_b, 1)]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Glicko-like confidence interval for rating
CREATE OR REPLACE FUNCTION substrate.rating_confidence(rd FLOAT8, n_games INT)
RETURNS FLOAT8 AS $$
import math
# RD decreases with games played, minimum ~30
return max(30, math.sqrt(rd**2 - (n_games * 15**2) if rd**2 > n_games * 15**2 else 900))
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== SIMULATED ANNEALING =====

-- SA acceptance probability: exp(-(new_cost - old_cost) / temperature)
CREATE OR REPLACE FUNCTION substrate.sa_accept(old_cost FLOAT8, new_cost FLOAT8, temperature FLOAT8)
RETURNS FLOAT8 AS $$
import math
if new_cost < old_cost: return 1.0
if temperature <= 0: return 0.0
return math.exp(-(new_cost - old_cost) / temperature)
$$ LANGUAGE plpython3u IMMUTABLE;

-- SA cooling schedule
CREATE OR REPLACE FUNCTION substrate.sa_temperature(
    initial_temp FLOAT8, step INT, schedule TEXT DEFAULT 'exponential',
    cooling_rate FLOAT8 DEFAULT 0.995
)
RETURNS FLOAT8 AS $$
import math
if schedule == 'exponential':
    return initial_temp * cooling_rate ** step
elif schedule == 'linear':
    return max(0.001, initial_temp - step * cooling_rate)
elif schedule == 'logarithmic':
    return initial_temp / (1 + math.log(1 + step))
return initial_temp * cooling_rate ** step
$$ LANGUAGE plpython3u IMMUTABLE;
