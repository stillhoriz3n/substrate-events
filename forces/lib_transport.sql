-- ============================================================
-- SUBSTRATE LIBRARY: lib.transport
-- Transport-layer primitives: connection math, flow control,
-- backpressure, keepalive, multiplexing, NAT, tunneling
-- ============================================================

-- ===== CONNECTION STATE =====

-- TCP state machine: valid transitions from a given state
CREATE OR REPLACE FUNCTION substrate.tcp_state_transitions(current_state TEXT)
RETURNS JSONB AS $$
import json
transitions = {
    'CLOSED':      {'active_open':'SYN_SENT','passive_open':'LISTEN'},
    'LISTEN':      {'recv_syn':'SYN_RCVD','send_syn':'SYN_SENT','close':'CLOSED'},
    'SYN_SENT':    {'recv_syn_ack':'ESTABLISHED','recv_syn':'SYN_RCVD','close':'CLOSED','timeout':'CLOSED'},
    'SYN_RCVD':    {'recv_ack':'ESTABLISHED','close':'FIN_WAIT_1','timeout':'CLOSED'},
    'ESTABLISHED': {'close':'FIN_WAIT_1','recv_fin':'CLOSE_WAIT'},
    'FIN_WAIT_1':  {'recv_ack':'FIN_WAIT_2','recv_fin':'CLOSING','recv_fin_ack':'TIME_WAIT'},
    'FIN_WAIT_2':  {'recv_fin':'TIME_WAIT'},
    'CLOSING':     {'recv_ack':'TIME_WAIT'},
    'TIME_WAIT':   {'timeout_2msl':'CLOSED'},
    'CLOSE_WAIT':  {'close':'LAST_ACK'},
    'LAST_ACK':    {'recv_ack':'CLOSED'},
}
t = transitions.get(current_state.upper())
return json.dumps(t) if t else json.dumps({'error':'unknown state','valid_states':list(transitions.keys())})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Connection state classifier from observed behavior
CREATE OR REPLACE FUNCTION substrate.connection_health(
    rtt_ms FLOAT8, packet_loss_pct FLOAT8, jitter_ms FLOAT8
)
RETURNS JSONB AS $$
import json
score = 100
if rtt_ms > 200: score -= 30
elif rtt_ms > 100: score -= 15
elif rtt_ms > 50: score -= 5
if packet_loss_pct > 5: score -= 40
elif packet_loss_pct > 1: score -= 20
elif packet_loss_pct > 0.1: score -= 5
if jitter_ms > 50: score -= 20
elif jitter_ms > 20: score -= 10
elif jitter_ms > 5: score -= 3
score = max(0, score)
if score >= 80: grade = 'excellent'
elif score >= 60: grade = 'good'
elif score >= 40: grade = 'degraded'
elif score >= 20: grade = 'poor'
else: grade = 'critical'
return json.dumps({'score':score,'grade':grade,'rtt_ms':rtt_ms,'loss_pct':packet_loss_pct,'jitter_ms':jitter_ms})
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== FLOW CONTROL =====

-- TCP receive window advertisement: buffer_size - unread_data
CREATE OR REPLACE FUNCTION substrate.recv_window(buffer_bytes INT, unread_bytes INT)
RETURNS INT AS $$ SELECT GREATEST(0, buffer_bytes - unread_bytes) $$ LANGUAGE sql IMMUTABLE;

-- Window scaling factor (RFC 7323): 2^shift
CREATE OR REPLACE FUNCTION substrate.window_scale(shift INT)
RETURNS BIGINT AS $$ SELECT power(2, shift)::bigint $$ LANGUAGE sql IMMUTABLE;

-- Effective window with scaling
CREATE OR REPLACE FUNCTION substrate.effective_window(advertised_window INT, scale_shift INT)
RETURNS BIGINT AS $$ SELECT (advertised_window::bigint) * power(2, scale_shift)::bigint $$ LANGUAGE sql IMMUTABLE;

-- Token bucket state: given rate, bucket_size, elapsed_sec, current_tokens, requested
-- Returns [tokens_remaining, allowed, wait_ms]
CREATE OR REPLACE FUNCTION substrate.token_bucket(
    rate_per_sec FLOAT8, bucket_size FLOAT8,
    elapsed_sec FLOAT8, current_tokens FLOAT8, requested FLOAT8
)
RETURNS FLOAT8[] AS $$
tokens = min(bucket_size, current_tokens + rate_per_sec * elapsed_sec)
if tokens >= requested:
    return [tokens - requested, 1.0, 0.0]
wait = (requested - tokens) / rate_per_sec * 1000
return [tokens, 0.0, wait]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Leaky bucket: steady output rate, returns queue depth after adding n_items
CREATE OR REPLACE FUNCTION substrate.leaky_bucket(
    queue_depth INT, max_depth INT,
    drain_rate_per_sec FLOAT8, elapsed_sec FLOAT8, add_items INT
)
RETURNS INT[] AS $$
drained = int(drain_rate_per_sec * elapsed_sec)
current = max(0, queue_depth - drained) + add_items
overflow = max(0, current - max_depth)
current = min(current, max_depth)
return [current, overflow]
$$ LANGUAGE plpython3u IMMUTABLE;

-- AIMD (Additive Increase Multiplicative Decrease) congestion window
CREATE OR REPLACE FUNCTION substrate.aimd_cwnd(
    current_cwnd FLOAT8, event TEXT,
    additive_inc FLOAT8 DEFAULT 1.0, multiplicative_dec FLOAT8 DEFAULT 0.5
)
RETURNS FLOAT8 AS $$
if event == 'ack':
    return current_cwnd + additive_inc / current_cwnd
elif event == 'loss':
    return max(1.0, current_cwnd * multiplicative_dec)
return current_cwnd
$$ LANGUAGE plpython3u IMMUTABLE;

-- Cubic congestion control: W(t) = C*(t-K)^3 + Wmax
CREATE OR REPLACE FUNCTION substrate.cubic_cwnd(
    t_sec FLOAT8, wmax FLOAT8,
    c_param FLOAT8 DEFAULT 0.4, beta FLOAT8 DEFAULT 0.7
)
RETURNS FLOAT8 AS $$
import math
K = (wmax * (1 - beta) / c_param) ** (1.0/3.0)
w = c_param * (t_sec - K)**3 + wmax
return max(1.0, w)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== BACKPRESSURE =====

-- Backpressure signal: given queue fill ratio, return action
CREATE OR REPLACE FUNCTION substrate.backpressure_action(
    queue_fill_ratio FLOAT8,
    low_watermark FLOAT8 DEFAULT 0.3,
    high_watermark FLOAT8 DEFAULT 0.8
)
RETURNS JSONB AS $$
import json
if queue_fill_ratio >= 0.95:
    return json.dumps({'action':'drop','reason':'queue_full','accept':False,'throttle_pct':100})
if queue_fill_ratio >= high_watermark:
    throttle = int((queue_fill_ratio - high_watermark) / (1 - high_watermark) * 100)
    return json.dumps({'action':'throttle','accept':True,'throttle_pct':throttle})
if queue_fill_ratio <= low_watermark:
    return json.dumps({'action':'resume','accept':True,'throttle_pct':0})
return json.dumps({'action':'normal','accept':True,'throttle_pct':0})
$$ LANGUAGE plpython3u IMMUTABLE;

-- RED (Random Early Detection): drop probability at given avg queue size
CREATE OR REPLACE FUNCTION substrate.red_drop_probability(
    avg_queue FLOAT8, min_thresh FLOAT8, max_thresh FLOAT8, max_prob FLOAT8 DEFAULT 0.1
)
RETURNS FLOAT8 AS $$
if avg_queue <= min_thresh: return 0.0
if avg_queue >= max_thresh: return 1.0
return max_prob * (avg_queue - min_thresh) / (max_thresh - min_thresh)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== KEEPALIVE / HEARTBEAT =====

-- Heartbeat interval recommendation based on expected failure detection time
CREATE OR REPLACE FUNCTION substrate.heartbeat_interval(
    target_detection_sec FLOAT8, n_missed_before_dead INT DEFAULT 3
)
RETURNS FLOAT8 AS $$ SELECT target_detection_sec / n_missed_before_dead $$ LANGUAGE sql IMMUTABLE;

-- Phi accrual failure detector: suspicion level from heartbeat history
CREATE OR REPLACE FUNCTION substrate.phi_accrual(
    intervals_ms FLOAT8[], last_heartbeat_age_ms FLOAT8
)
RETURNS FLOAT8 AS $$
import math, statistics
if len(intervals_ms) < 2: return 0
mu = statistics.mean(intervals_ms)
sigma = statistics.stdev(intervals_ms)
if sigma == 0: sigma = 0.1
y = (last_heartbeat_age_ms - mu) / sigma
# Approximate CDF of normal distribution
cdf = 0.5 * (1 + math.erf(y / math.sqrt(2)))
if cdf >= 1.0: return 16.0  # cap
if cdf <= 0.0: return 0.0
return -math.log10(1 - cdf)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Exponential backoff with jitter for reconnection
CREATE OR REPLACE FUNCTION substrate.reconnect_delay_ms(
    attempt INT, base_ms INT DEFAULT 1000,
    max_ms INT DEFAULT 60000, jitter_pct FLOAT8 DEFAULT 0.25
)
RETURNS INT AS $$
import random, math
delay = min(max_ms, base_ms * (2 ** attempt))
jitter = int(delay * jitter_pct * (random.random() * 2 - 1))
return max(0, delay + jitter)
$$ LANGUAGE plpython3u;

-- ===== MULTIPLEXING =====

-- Stream weight to bandwidth share (HTTP/2 style)
CREATE OR REPLACE FUNCTION substrate.stream_bandwidth_share(
    stream_weight INT, total_weight INT, available_bps FLOAT8
)
RETURNS FLOAT8 AS $$ SELECT available_bps * stream_weight::float8 / NULLIF(total_weight, 0) $$ LANGUAGE sql IMMUTABLE;

-- HTTP/2 stream priority: dependency tree weight calculation
CREATE OR REPLACE FUNCTION substrate.h2_effective_weight(
    own_weight INT, parent_weight INT, parent_share FLOAT8
)
RETURNS FLOAT8 AS $$ SELECT parent_share * own_weight::float8 / 256.0 $$ LANGUAGE sql IMMUTABLE;

-- QUIC stream ID decoder: type from stream ID
CREATE OR REPLACE FUNCTION substrate.quic_stream_type(stream_id BIGINT)
RETURNS JSONB AS $$
import json
initiator = 'client' if stream_id % 2 == 0 else 'server'
direction = 'bidirectional' if (stream_id >> 1) % 2 == 0 else 'unidirectional'
return json.dumps({'stream_id':stream_id,'initiator':initiator,'direction':direction})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Connection multiplexing overhead: n_streams * per_stream_bytes + base
CREATE OR REPLACE FUNCTION substrate.mux_overhead(
    n_streams INT, per_stream_bytes INT DEFAULT 64, base_bytes INT DEFAULT 256
)
RETURNS INT AS $$ SELECT base_bytes + n_streams * per_stream_bytes $$ LANGUAGE sql IMMUTABLE;

-- ===== NAT TRAVERSAL =====

-- STUN XOR-mapped address encoding (RFC 5389)
CREATE OR REPLACE FUNCTION substrate.stun_xor_address(ip TEXT, port INT, magic_cookie BIGINT DEFAULT 554869826)
RETURNS JSONB AS $$
import json
parts = [int(x) for x in ip.split('.')]
ip_int = (parts[0]<<24) + (parts[1]<<16) + (parts[2]<<8) + parts[3]
xor_port = port ^ (magic_cookie >> 16)
xor_ip = ip_int ^ magic_cookie
xor_parts = [(xor_ip>>24)&255, (xor_ip>>16)&255, (xor_ip>>8)&255, xor_ip&255]
return json.dumps({
    'xor_port': xor_port,
    'xor_address': f'{xor_parts[0]}.{xor_parts[1]}.{xor_parts[2]}.{xor_parts[3]}',
    'original_ip': ip,
    'original_port': port
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- NAT type classifier from observed behavior
CREATE OR REPLACE FUNCTION substrate.nat_type(
    same_ip_same_port BOOLEAN,
    same_ip_diff_port BOOLEAN,
    diff_ip BOOLEAN
)
RETURNS TEXT AS $$
if same_ip_same_port and same_ip_diff_port and diff_ip:
    return 'full_cone'
elif same_ip_same_port and same_ip_diff_port:
    return 'restricted_cone'
elif same_ip_same_port:
    return 'port_restricted_cone'
else:
    return 'symmetric'
$$ LANGUAGE plpython3u IMMUTABLE;

-- ICE candidate priority (RFC 8445)
CREATE OR REPLACE FUNCTION substrate.ice_priority(type_pref INT, local_pref INT, component_id INT)
RETURNS BIGINT AS $$ SELECT ((type_pref::bigint) << 24) + ((local_pref::bigint) << 8) + (256 - component_id) $$ LANGUAGE sql IMMUTABLE;

-- ICE candidate type preferences
CREATE OR REPLACE FUNCTION substrate.ice_type_pref(candidate_type TEXT)
RETURNS INT AS $$
prefs = {'host': 126, 'srflx': 100, 'prflx': 110, 'relay': 0}
return prefs.get(candidate_type.lower(), 0)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== TUNNELING =====

-- WireGuard overhead per packet (bytes)
CREATE OR REPLACE FUNCTION substrate.wireguard_overhead()
RETURNS INT AS $$ SELECT 60 $$ LANGUAGE sql IMMUTABLE;
-- 20 IP + 8 UDP + 4 type + 4 receiver + 8 counter + 16 AEAD tag = 60

-- WireGuard effective MTU
CREATE OR REPLACE FUNCTION substrate.wireguard_mtu(outer_mtu INT DEFAULT 1500)
RETURNS INT AS $$ SELECT outer_mtu - 60 $$ LANGUAGE sql IMMUTABLE;

-- IPsec overhead (ESP transport mode, AES-GCM)
CREATE OR REPLACE FUNCTION substrate.ipsec_overhead(mode TEXT DEFAULT 'transport')
RETURNS INT AS $$
if mode == 'transport': return 50   # ESP header(8) + IV(8) + padding(2) + ICV(16) + next_hdr(1) + pad
if mode == 'tunnel': return 70      # + outer IP header (20)
return 50
$$ LANGUAGE plpython3u IMMUTABLE;

-- VXLAN overhead
CREATE OR REPLACE FUNCTION substrate.vxlan_overhead()
RETURNS INT AS $$ SELECT 50 $$ LANGUAGE sql IMMUTABLE;
-- 14 outer Eth + 20 outer IP + 8 UDP + 8 VXLAN = 50

-- GRE overhead
CREATE OR REPLACE FUNCTION substrate.gre_overhead(with_key BOOLEAN DEFAULT FALSE, with_seq BOOLEAN DEFAULT FALSE)
RETURNS INT AS $$ SELECT 4 + CASE WHEN with_key THEN 4 ELSE 0 END + CASE WHEN with_seq THEN 4 ELSE 0 END $$ LANGUAGE sql IMMUTABLE;

-- Effective payload after tunnel encapsulation
CREATE OR REPLACE FUNCTION substrate.tunnel_payload(
    outer_mtu INT DEFAULT 1500, tunnel_type TEXT DEFAULT 'wireguard'
)
RETURNS INT AS $$
overheads = {
    'wireguard': 60,
    'ipsec_transport': 50,
    'ipsec_tunnel': 70,
    'vxlan': 50,
    'gre': 4,
    'gre_key': 8,
    'geneve': 50,
    'ipip': 20,
    'sit': 20,
}
oh = overheads.get(tunnel_type.lower(), 60)
return outer_mtu - oh
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== DISCOVERY =====

-- mDNS service name construction
CREATE OR REPLACE FUNCTION substrate.mdns_service_name(instance TEXT, service_type TEXT, domain TEXT DEFAULT 'local')
RETURNS TEXT AS $$ SELECT instance || '.' || service_type || '.' || domain $$ LANGUAGE sql IMMUTABLE;

-- DNS-SD TXT record builder (key=value pairs to wire format length)
CREATE OR REPLACE FUNCTION substrate.dnssd_txt_size(pairs JSONB)
RETURNS INT AS $$
import json
obj = json.loads(pairs)
total = 0
for k, v in obj.items():
    entry = f'{k}={v}'
    total += 1 + len(entry.encode('utf-8'))  # length byte + data
return total
$$ LANGUAGE plpython3u IMMUTABLE;

-- Multicast group for service type (deterministic from name)
CREATE OR REPLACE FUNCTION substrate.service_multicast_group(service_name TEXT)
RETURNS TEXT AS $$
import hashlib
h = int(hashlib.md5(service_name.encode()).hexdigest(), 16)
# Map to 239.0.0.0/8 local multicast range
b2 = (h >> 16) & 255
b3 = (h >> 8) & 255
b4 = h & 255
return f'239.{b2}.{b3}.{b4}'
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== TIMING =====

-- Round-trip time estimation (Jacobson/Karels algorithm)
CREATE OR REPLACE FUNCTION substrate.rtt_estimate(
    srtt FLOAT8, rttvar FLOAT8, measured_rtt FLOAT8,
    alpha FLOAT8 DEFAULT 0.125, beta FLOAT8 DEFAULT 0.25
)
RETURNS FLOAT8[] AS $$
new_rttvar = (1 - beta) * rttvar + beta * abs(srtt - measured_rtt)
new_srtt = (1 - alpha) * srtt + alpha * measured_rtt
rto = new_srtt + max(1.0, 4 * new_rttvar)  # retransmission timeout
return [new_srtt, new_rttvar, rto]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Propagation delay estimate: distance_km / (speed_of_light * 2/3 for fiber)
CREATE OR REPLACE FUNCTION substrate.propagation_delay_ms(distance_km FLOAT8, medium TEXT DEFAULT 'fiber')
RETURNS FLOAT8 AS $$
speeds = {'fiber': 200000, 'copper': 200000, 'wireless': 300000, 'satellite': 300000}
speed = speeds.get(medium.lower(), 200000)  # km/s
return (distance_km / speed) * 1000
$$ LANGUAGE plpython3u IMMUTABLE;

-- End-to-end latency budget breakdown
CREATE OR REPLACE FUNCTION substrate.latency_budget(
    propagation_ms FLOAT8, serialization_ms FLOAT8,
    processing_ms FLOAT8, queuing_ms FLOAT8
)
RETURNS JSONB AS $$
import json
total = propagation_ms + serialization_ms + processing_ms + queuing_ms
return json.dumps({
    'total_ms': round(total, 2),
    'propagation_ms': propagation_ms,
    'serialization_ms': serialization_ms,
    'processing_ms': processing_ms,
    'queuing_ms': queuing_ms,
    'breakdown_pct': {
        'propagation': round(100*propagation_ms/total, 1) if total > 0 else 0,
        'serialization': round(100*serialization_ms/total, 1) if total > 0 else 0,
        'processing': round(100*processing_ms/total, 1) if total > 0 else 0,
        'queuing': round(100*queuing_ms/total, 1) if total > 0 else 0,
    }
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Serialization delay: packet_size_bytes / link_rate_bps
CREATE OR REPLACE FUNCTION substrate.serialization_delay_ms(packet_bytes INT, link_rate_mbps FLOAT8)
RETURNS FLOAT8 AS $$ SELECT (packet_bytes * 8.0) / (link_rate_mbps * 1000) $$ LANGUAGE sql IMMUTABLE;
