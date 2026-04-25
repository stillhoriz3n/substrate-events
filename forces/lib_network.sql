-- ============================================================
-- SUBSTRATE LIBRARY: lib.network
-- IP math, CIDR, bandwidth, latency, queue theory
-- ============================================================

-- IP to integer
CREATE OR REPLACE FUNCTION substrate.ip_to_int(ip TEXT)
RETURNS BIGINT AS $$
parts = ip.strip().split('.')
return (int(parts[0])<<24) + (int(parts[1])<<16) + (int(parts[2])<<8) + int(parts[3])
$$ LANGUAGE plpython3u IMMUTABLE;

-- Integer to IP
CREATE OR REPLACE FUNCTION substrate.int_to_ip(n BIGINT)
RETURNS TEXT AS $$
return f'{(n>>24)&255}.{(n>>16)&255}.{(n>>8)&255}.{n&255}'
$$ LANGUAGE plpython3u IMMUTABLE;

-- CIDR: network address
CREATE OR REPLACE FUNCTION substrate.cidr_network(cidr TEXT)
RETURNS TEXT AS $$
ip, bits = cidr.split('/')
bits = int(bits)
parts = [int(x) for x in ip.split('.')]
n = (parts[0]<<24)+(parts[1]<<16)+(parts[2]<<8)+parts[3]
mask = (0xFFFFFFFF << (32-bits)) & 0xFFFFFFFF
net = n & mask
return f'{(net>>24)&255}.{(net>>16)&255}.{(net>>8)&255}.{net&255}'
$$ LANGUAGE plpython3u IMMUTABLE;

-- CIDR: broadcast address
CREATE OR REPLACE FUNCTION substrate.cidr_broadcast(cidr TEXT)
RETURNS TEXT AS $$
ip, bits = cidr.split('/')
bits = int(bits)
parts = [int(x) for x in ip.split('.')]
n = (parts[0]<<24)+(parts[1]<<16)+(parts[2]<<8)+parts[3]
mask = (0xFFFFFFFF << (32-bits)) & 0xFFFFFFFF
bcast = (n & mask) | (~mask & 0xFFFFFFFF)
return f'{(bcast>>24)&255}.{(bcast>>16)&255}.{(bcast>>8)&255}.{bcast&255}'
$$ LANGUAGE plpython3u IMMUTABLE;

-- CIDR: number of usable hosts
CREATE OR REPLACE FUNCTION substrate.cidr_hosts(cidr TEXT)
RETURNS BIGINT AS $$
bits = int(cidr.split('/')[1])
if bits >= 31: return 2**(32-bits)
return 2**(32-bits) - 2
$$ LANGUAGE plpython3u IMMUTABLE;

-- Is IP in CIDR?
CREATE OR REPLACE FUNCTION substrate.ip_in_cidr(ip TEXT, cidr TEXT)
RETURNS BOOLEAN AS $$
cidr_ip, bits = cidr.split('/')
bits = int(bits)
def to_int(s):
    p = [int(x) for x in s.strip().split('.')]
    return (p[0]<<24)+(p[1]<<16)+(p[2]<<8)+p[3]
mask = (0xFFFFFFFF << (32-bits)) & 0xFFFFFFFF
return (to_int(ip) & mask) == (to_int(cidr_ip) & mask)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Subnet mask from prefix length
CREATE OR REPLACE FUNCTION substrate.prefix_to_mask(prefix_len INT)
RETURNS TEXT AS $$
mask = (0xFFFFFFFF << (32-prefix_len)) & 0xFFFFFFFF
return f'{(mask>>24)&255}.{(mask>>16)&255}.{(mask>>8)&255}.{mask&255}'
$$ LANGUAGE plpython3u IMMUTABLE;

-- Bandwidth-delay product (bytes): bandwidth in Mbps, rtt in ms
CREATE OR REPLACE FUNCTION substrate.bdp(bandwidth_mbps FLOAT8, rtt_ms FLOAT8)
RETURNS FLOAT8 AS $$ SELECT (bandwidth_mbps * 1e6 / 8) * (rtt_ms / 1000) $$ LANGUAGE sql IMMUTABLE;

-- Transfer time estimate (seconds): size_bytes, bandwidth_mbps
CREATE OR REPLACE FUNCTION substrate.transfer_time(size_bytes BIGINT, bandwidth_mbps FLOAT8)
RETURNS FLOAT8 AS $$ SELECT size_bytes::float8 / (bandwidth_mbps * 1e6 / 8) $$ LANGUAGE sql IMMUTABLE;

-- Effective throughput with packet loss (Mathis formula): MSS bytes, RTT ms, loss rate 0..1
CREATE OR REPLACE FUNCTION substrate.mathis_throughput(mss INT DEFAULT 1460, rtt_ms FLOAT8 DEFAULT 10, loss_rate FLOAT8 DEFAULT 0.01)
RETURNS FLOAT8 AS $$
import math
if loss_rate <= 0: return float('inf')
return (mss / (rtt_ms/1000.0)) * (1.0 / math.sqrt(loss_rate))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Little's Law: L = λ * W (items_in_system = arrival_rate * avg_wait)
CREATE OR REPLACE FUNCTION substrate.littles_law_l(arrival_rate FLOAT8, avg_wait FLOAT8)
RETURNS FLOAT8 AS $$ SELECT arrival_rate * avg_wait $$ LANGUAGE sql IMMUTABLE;

-- Little's Law solve for W
CREATE OR REPLACE FUNCTION substrate.littles_law_w(items_in_system FLOAT8, arrival_rate FLOAT8)
RETURNS FLOAT8 AS $$ SELECT items_in_system / NULLIF(arrival_rate, 0) $$ LANGUAGE sql IMMUTABLE;

-- M/M/1 queue: utilization, avg queue length, avg wait time
-- arrival_rate (λ), service_rate (μ)
CREATE OR REPLACE FUNCTION substrate.mm1_queue(arrival_rate FLOAT8, service_rate FLOAT8)
RETURNS TABLE(utilization FLOAT8, avg_queue_len FLOAT8, avg_wait_time FLOAT8, avg_system_time FLOAT8) AS $$
rho = arrival_rate / service_rate
if rho >= 1:
    return [(rho, float('inf'), float('inf'), float('inf'))]
lq = rho**2 / (1 - rho)
wq = lq / arrival_rate
ws = wq + 1/service_rate
return [(rho, lq, wq, ws)]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Erlang C: probability of queuing given c servers, load A (in Erlangs)
CREATE OR REPLACE FUNCTION substrate.erlang_c(servers INT, load_erlangs FLOAT8)
RETURNS FLOAT8 AS $$
import math
c = servers; A = load_erlangs
if A >= c: return 1.0
Ac_over_cfact = A**c / math.factorial(c)
s = sum(A**k / math.factorial(k) for k in range(c))
return Ac_over_cfact / (Ac_over_cfact + (1 - A/c) * s)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Human-readable byte size
CREATE OR REPLACE FUNCTION substrate.human_bytes(nbytes BIGINT)
RETURNS TEXT AS $$
val = float(nbytes)
for unit in ['B','KB','MB','GB','TB','PB']:
    if abs(val) < 1024.0 or unit == 'PB':
        return f'{val:.1f} {unit}'
    val /= 1024.0
return f'{val:.1f} PB'
$$ LANGUAGE plpython3u IMMUTABLE;

-- Parse human byte string to integer (e.g., '1.5 GB' → 1610612736)
CREATE OR REPLACE FUNCTION substrate.parse_bytes(input TEXT)
RETURNS BIGINT AS $$
import re
m = re.match(r'([\d.]+)\s*(B|KB|MB|GB|TB|PB|KiB|MiB|GiB|TiB|PiB)', input.strip(), re.I)
if not m: return None
val = float(m.group(1))
unit = m.group(2).upper()
mult = {'B':1,'KB':1024,'KIB':1024,'MB':1024**2,'MIB':1024**2,'GB':1024**3,'GIB':1024**3,'TB':1024**4,'TIB':1024**4,'PB':1024**5,'PIB':1024**5}
return int(val * mult.get(unit, 1))
$$ LANGUAGE plpython3u IMMUTABLE;
