-- ============================================================
-- SUBSTRATE LIBRARY: lib.system
-- Capacity planning, resource estimation, ops formulas
-- ============================================================

-- Amdahl's law: max speedup given parallel fraction p and n processors
CREATE OR REPLACE FUNCTION substrate.amdahl(parallel_fraction FLOAT8, n_processors INT)
RETURNS FLOAT8 AS $$ SELECT 1.0 / ((1 - parallel_fraction) + parallel_fraction / n_processors) $$ LANGUAGE sql IMMUTABLE;

-- Gustafson's law: scaled speedup
CREATE OR REPLACE FUNCTION substrate.gustafson(parallel_fraction FLOAT8, n_processors INT)
RETURNS FLOAT8 AS $$ SELECT n_processors - (1 - parallel_fraction) * (n_processors - 1) $$ LANGUAGE sql IMMUTABLE;

-- IOPS to throughput: iops * block_size_bytes
CREATE OR REPLACE FUNCTION substrate.iops_to_mbps(iops INT, block_size_kb INT DEFAULT 4)
RETURNS FLOAT8 AS $$ SELECT (iops::float8 * block_size_kb) / 1024 $$ LANGUAGE sql IMMUTABLE;

-- Disk RAID capacity: n_disks, disk_size_gb, raid_level
CREATE OR REPLACE FUNCTION substrate.raid_capacity(n_disks INT, disk_gb FLOAT8, raid_level TEXT)
RETURNS FLOAT8 AS $$
r = raid_level.upper()
if r == '0': return n_disks * disk_gb
if r == '1': return (n_disks // 2) * disk_gb
if r == '5': return (n_disks - 1) * disk_gb
if r == '6': return (n_disks - 2) * disk_gb
if r in ('10', '1+0'): return (n_disks // 2) * disk_gb
return n_disks * disk_gb
$$ LANGUAGE plpython3u IMMUTABLE;

-- Memory page calculation: data_size_bytes / page_size
CREATE OR REPLACE FUNCTION substrate.pages_needed(data_bytes BIGINT, page_size INT DEFAULT 4096)
RETURNS BIGINT AS $$ SELECT (data_bytes + page_size - 1) / page_size $$ LANGUAGE sql IMMUTABLE;

-- Container memory estimate: base_mb + (per_request_kb * concurrency)
CREATE OR REPLACE FUNCTION substrate.container_mem_mb(base_mb INT, per_req_kb FLOAT8, concurrency INT)
RETURNS FLOAT8 AS $$ SELECT base_mb + (per_req_kb * concurrency / 1024.0) $$ LANGUAGE sql IMMUTABLE;

-- CPU utilization percentage from /proc/stat style deltas
CREATE OR REPLACE FUNCTION substrate.cpu_util(user_delta BIGINT, system_delta BIGINT, idle_delta BIGINT)
RETURNS FLOAT8 AS $$
SELECT CASE WHEN (user_delta + system_delta + idle_delta) = 0 THEN 0
ELSE 100.0 * (user_delta + system_delta)::float8 / (user_delta + system_delta + idle_delta) END
$$ LANGUAGE sql IMMUTABLE;

-- Load average interpretation: load / n_cores
CREATE OR REPLACE FUNCTION substrate.load_ratio(load_avg FLOAT8, n_cores INT)
RETURNS FLOAT8 AS $$ SELECT load_avg / GREATEST(n_cores, 1) $$ LANGUAGE sql IMMUTABLE;

-- SLA uptime to allowed downtime per period
CREATE OR REPLACE FUNCTION substrate.sla_downtime(nines FLOAT8, period TEXT DEFAULT 'month')
RETURNS TEXT AS $$
uptime = 1 - 10**(-nines)
periods = {'year': 365.25*86400, 'month': 30*86400, 'week': 7*86400, 'day': 86400}
total_secs = periods.get(period.lower(), 86400)
down_secs = total_secs * (1 - uptime)
if down_secs < 1: return f'{down_secs*1000:.1f}ms'
if down_secs < 60: return f'{down_secs:.1f}s'
if down_secs < 3600: return f'{down_secs/60:.1f}m'
return f'{down_secs/3600:.1f}h'
$$ LANGUAGE plpython3u IMMUTABLE;

-- Error budget: given SLA % and current uptime %, return remaining budget in seconds per period
CREATE OR REPLACE FUNCTION substrate.error_budget(sla_pct FLOAT8, current_uptime_pct FLOAT8, period_secs FLOAT8 DEFAULT 2592000)
RETURNS FLOAT8 AS $$
SELECT (current_uptime_pct - sla_pct) / 100.0 * period_secs
$$ LANGUAGE sql IMMUTABLE;

-- Compound growth / CAGR
CREATE OR REPLACE FUNCTION substrate.cagr(start_val FLOAT8, end_val FLOAT8, n_periods INT)
RETURNS FLOAT8 AS $$
import math
if start_val <= 0 or n_periods <= 0: return 0
return (end_val / start_val) ** (1.0 / n_periods) - 1
$$ LANGUAGE plpython3u IMMUTABLE;

-- Exponential growth projection
CREATE OR REPLACE FUNCTION substrate.growth_project(current_val FLOAT8, growth_rate FLOAT8, n_periods INT)
RETURNS FLOAT8 AS $$ SELECT current_val * power(1 + growth_rate, n_periods) $$ LANGUAGE sql IMMUTABLE;

-- Break-even analysis: fixed_cost / (price - variable_cost)
CREATE OR REPLACE FUNCTION substrate.break_even(fixed_cost FLOAT8, price_per_unit FLOAT8, variable_cost_per_unit FLOAT8)
RETURNS FLOAT8 AS $$ SELECT fixed_cost / NULLIF(price_per_unit - variable_cost_per_unit, 0) $$ LANGUAGE sql IMMUTABLE;

-- Token cost estimator: tokens * price_per_million / 1e6
CREATE OR REPLACE FUNCTION substrate.token_cost(n_tokens INT, price_per_million FLOAT8 DEFAULT 15.0)
RETURNS FLOAT8 AS $$ SELECT n_tokens::float8 * price_per_million / 1000000.0 $$ LANGUAGE sql IMMUTABLE;

-- Retry backoff calculator: base_ms * multiplier^attempt (capped)
CREATE OR REPLACE FUNCTION substrate.backoff_ms(attempt INT, base_ms INT DEFAULT 100, multiplier FLOAT8 DEFAULT 2.0, max_ms INT DEFAULT 30000)
RETURNS INT AS $$ SELECT LEAST(max_ms, (base_ms * power(multiplier, attempt))::int) $$ LANGUAGE sql IMMUTABLE;

-- Connection pool sizing (based on PostgreSQL wiki formula)
-- connections = ((core_count * 2) + effective_spindle_count)
CREATE OR REPLACE FUNCTION substrate.pool_size(cores INT, spindles INT DEFAULT 1)
RETURNS INT AS $$ SELECT cores * 2 + spindles $$ LANGUAGE sql IMMUTABLE;
