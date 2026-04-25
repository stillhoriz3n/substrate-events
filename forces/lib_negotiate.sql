-- ============================================================
-- SUBSTRATE LIBRARY: lib.negotiate
-- Handshake, capability exchange, version negotiation,
-- session management, auth, connection lifecycle
-- ============================================================

-- ===== SEMVER =====

-- Parse semver string to components
CREATE OR REPLACE FUNCTION substrate.semver_parse(ver TEXT)
RETURNS JSONB AS $$
import json, re
m = re.match(r'^v?(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.]+))?(?:\+([a-zA-Z0-9.]+))?$', ver.strip())
if not m: return json.dumps({'error': 'invalid semver', 'input': ver})
return json.dumps({
    'major': int(m.group(1)), 'minor': int(m.group(2)), 'patch': int(m.group(3)),
    'prerelease': m.group(4), 'build': m.group(5)
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Compare two semver strings: returns -1, 0, or 1
CREATE OR REPLACE FUNCTION substrate.semver_compare(a TEXT, b TEXT)
RETURNS INT AS $$
import re
def parse(v):
    m = re.match(r'^v?(\d+)\.(\d+)\.(\d+)', v.strip())
    if not m: return (0,0,0)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
va, vb = parse(a), parse(b)
if va < vb: return -1
if va > vb: return 1
return 0
$$ LANGUAGE plpython3u IMMUTABLE;

-- Check if version satisfies a range (simplified: ^major.minor, ~major.minor.patch, >=, exact)
CREATE OR REPLACE FUNCTION substrate.semver_satisfies(ver TEXT, range_spec TEXT)
RETURNS BOOLEAN AS $$
import re
def parse(v):
    m = re.match(r'^v?(\d+)\.(\d+)\.(\d+)', v.strip())
    if not m: return (0,0,0)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
v = parse(ver)
spec = range_spec.strip()
if spec.startswith('>='):
    return v >= parse(spec[2:])
if spec.startswith('>'):
    return v > parse(spec[1:])
if spec.startswith('<='):
    return v <= parse(spec[2:])
if spec.startswith('<'):
    return v < parse(spec[1:])
if spec.startswith('^'):
    target = parse(spec[1:])
    return v[0] == target[0] and v >= target
if spec.startswith('~'):
    target = parse(spec[1:])
    return v[0] == target[0] and v[1] == target[1] and v >= target
return v == parse(spec)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== CAPABILITY EXCHANGE =====

-- Capability bitmask: encode list of named caps to integer
CREATE OR REPLACE FUNCTION substrate.caps_encode(capabilities TEXT[], known_caps TEXT[])
RETURNS BIGINT AS $$
mask = 0
for cap in capabilities:
    if cap in known_caps:
        mask |= (1 << known_caps.index(cap))
return mask
$$ LANGUAGE plpython3u IMMUTABLE;

-- Capability bitmask decode
CREATE OR REPLACE FUNCTION substrate.caps_decode(mask BIGINT, known_caps TEXT[])
RETURNS TEXT[] AS $$
result = []
for i, cap in enumerate(known_caps):
    if mask & (1 << i):
        result.append(cap)
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- Capability intersection (what both peers support)
CREATE OR REPLACE FUNCTION substrate.caps_intersect(a_caps TEXT[], b_caps TEXT[])
RETURNS TEXT[] AS $$
sa = set(a_caps or [])
sb = set(b_caps or [])
return sorted(sa & sb)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Capability negotiation: given client and server caps, return negotiated set + missing
CREATE OR REPLACE FUNCTION substrate.caps_negotiate(client_caps TEXT[], server_caps TEXT[], required_caps TEXT[] DEFAULT ARRAY[]::TEXT[])
RETURNS JSONB AS $$
import json
client = set(client_caps or [])
server = set(server_caps or [])
required = set(required_caps or [])
agreed = sorted(client & server)
missing_required = sorted(required - (client & server))
client_only = sorted(client - server)
server_only = sorted(server - client)
return json.dumps({
    'agreed': agreed,
    'missing_required': missing_required,
    'compatible': len(missing_required) == 0,
    'client_unsupported': client_only,
    'server_unsupported': server_only
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Substrate standard capabilities
CREATE OR REPLACE FUNCTION substrate.standard_caps()
RETURNS TEXT[] AS $$
SELECT ARRAY[
    'blob.read', 'blob.write', 'blob.subscribe',
    'signal.emit', 'signal.listen',
    'force.execute', 'force.install',
    'manifest.publish', 'manifest.pull',
    'radiate', 'sync',
    'compress.zstd', 'compress.gzip',
    'encrypt.aes256', 'encrypt.chacha20',
    'transport.tcp', 'transport.wireguard', 'transport.http',
    'auth.token', 'auth.mtls',
    'crdt.gcounter', 'crdt.lww',
    'plpython3u'
]
$$ LANGUAGE sql IMMUTABLE;

-- ===== HANDSHAKE =====

-- Generate handshake init message
CREATE OR REPLACE FUNCTION substrate.handshake_init(
    peer_id TEXT, capabilities TEXT[],
    protocol_version TEXT DEFAULT '1.0.0',
    blob_count INT DEFAULT 0
)
RETURNS JSONB AS $$
import json, time, hashlib, os
nonce = os.urandom(16).hex()
return json.dumps({
    'type': 'handshake_init',
    'protocol_version': protocol_version,
    'peer_id': peer_id,
    'capabilities': capabilities,
    'blob_count': blob_count,
    'nonce': nonce,
    'timestamp': time.time()
})
$$ LANGUAGE plpython3u;

-- Validate and generate handshake response
CREATE OR REPLACE FUNCTION substrate.handshake_respond(
    init_msg JSONB, my_peer_id TEXT, my_caps TEXT[],
    min_version TEXT DEFAULT '1.0.0'
)
RETURNS JSONB AS $$
import json, time, os
msg = json.loads(init_msg)
their_ver = msg.get('protocol_version', '0.0.0')
# Version check
import re
def parse_ver(v):
    m = re.match(r'(\d+)\.(\d+)\.(\d+)', v)
    return tuple(int(x) for x in m.groups()) if m else (0,0,0)
if parse_ver(their_ver) < parse_ver(min_version):
    return json.dumps({
        'type': 'handshake_reject',
        'reason': f'protocol_version {their_ver} < {min_version}',
        'peer_id': my_peer_id
    })
their_caps = msg.get('capabilities', [])
my_set = set(my_caps or [])
their_set = set(their_caps)
agreed = sorted(my_set & their_set)
nonce = os.urandom(16).hex()
return json.dumps({
    'type': 'handshake_accept',
    'protocol_version': their_ver,
    'peer_id': my_peer_id,
    'remote_peer_id': msg.get('peer_id'),
    'agreed_capabilities': agreed,
    'nonce': nonce,
    'in_reply_to_nonce': msg.get('nonce'),
    'timestamp': time.time()
})
$$ LANGUAGE plpython3u;

-- ===== SESSION MANAGEMENT =====

-- Generate session token (HMAC-based)
CREATE OR REPLACE FUNCTION substrate.session_token(peer_id TEXT, secret TEXT, ttl_sec INT DEFAULT 3600)
RETURNS JSONB AS $$
import json, time, hmac, hashlib
now = time.time()
expires = now + ttl_sec
payload = f'{peer_id}:{int(expires)}'
sig = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
return json.dumps({
    'token': f'{payload}:{sig}',
    'peer_id': peer_id,
    'expires_at': expires,
    'ttl_sec': ttl_sec
})
$$ LANGUAGE plpython3u;

-- Validate session token
CREATE OR REPLACE FUNCTION substrate.session_validate(token TEXT, secret TEXT)
RETURNS JSONB AS $$
import json, time, hmac, hashlib
parts = token.split(':')
if len(parts) != 3:
    return json.dumps({'valid': False, 'reason': 'malformed'})
peer_id, expires_str, sig = parts
payload = f'{peer_id}:{expires_str}'
expected = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
if not hmac.compare_digest(sig, expected):
    return json.dumps({'valid': False, 'reason': 'bad_signature'})
if time.time() > int(expires_str):
    return json.dumps({'valid': False, 'reason': 'expired', 'peer_id': peer_id})
return json.dumps({'valid': True, 'peer_id': peer_id, 'expires_at': int(expires_str)})
$$ LANGUAGE plpython3u;

-- ===== CONNECTION LIFECYCLE =====

-- Connection state machine
CREATE OR REPLACE FUNCTION substrate.conn_state_transitions(current_state TEXT)
RETURNS JSONB AS $$
import json
transitions = {
    'disconnected':  {'connect': 'connecting'},
    'connecting':    {'handshake_ok': 'authenticating', 'timeout': 'backoff', 'error': 'backoff'},
    'authenticating':{'auth_ok': 'syncing', 'auth_fail': 'disconnected', 'timeout': 'backoff'},
    'syncing':       {'sync_done': 'active', 'error': 'degraded', 'timeout': 'degraded'},
    'active':        {'error': 'degraded', 'heartbeat_miss': 'suspect', 'close': 'draining'},
    'degraded':      {'recover': 'active', 'too_many_errors': 'backoff', 'close': 'draining'},
    'suspect':       {'heartbeat_ok': 'active', 'confirmed_dead': 'backoff'},
    'draining':      {'drained': 'disconnected'},
    'backoff':       {'timer_expired': 'connecting', 'give_up': 'disconnected'},
}
t = transitions.get(current_state.lower())
return json.dumps(t) if t else json.dumps({'error': 'unknown state', 'valid': list(transitions.keys())})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Connection lifecycle event logger format
CREATE OR REPLACE FUNCTION substrate.conn_event(
    peer_id TEXT, from_state TEXT, to_state TEXT, trigger TEXT,
    detail TEXT DEFAULT NULL
)
RETURNS JSONB AS $$
import json, time
evt = {
    'type': 'conn_event',
    'peer_id': peer_id,
    'from': from_state,
    'to': to_state,
    'trigger': trigger,
    'timestamp': time.time()
}
if detail: evt['detail'] = detail
return json.dumps(evt)
$$ LANGUAGE plpython3u;

-- ===== FEATURE FLAGS =====

-- Feature flag check: given a flag set (JSONB) and a flag name
CREATE OR REPLACE FUNCTION substrate.feature_enabled(flags JSONB, flag_name TEXT)
RETURNS BOOLEAN AS $$
import json
f = json.loads(flags)
return bool(f.get(flag_name, False))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Feature flag percentage rollout check
CREATE OR REPLACE FUNCTION substrate.feature_rollout(
    flag_name TEXT, peer_id TEXT, rollout_pct INT DEFAULT 100
)
RETURNS BOOLEAN AS $$
import hashlib
h = int(hashlib.md5(f'{flag_name}:{peer_id}'.encode()).hexdigest(), 16)
return (h % 100) < rollout_pct
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== PROTOCOL UPGRADE =====

-- Protocol upgrade negotiation: given offered protocols, return best match
CREATE OR REPLACE FUNCTION substrate.protocol_upgrade(
    offered TEXT[], supported TEXT[], preference_order TEXT[] DEFAULT NULL
)
RETURNS JSONB AS $$
import json
off = set(offered or [])
sup = set(supported or [])
candidates = sorted(off & sup)
if not candidates:
    return json.dumps({'upgrade': False, 'reason': 'no_common_protocol'})
if preference_order:
    for p in preference_order:
        if p in candidates:
            return json.dumps({'upgrade': True, 'selected': p, 'alternatives': candidates})
return json.dumps({'upgrade': True, 'selected': candidates[0], 'alternatives': candidates})
$$ LANGUAGE plpython3u IMMUTABLE;

-- ALPN (Application-Layer Protocol Negotiation) helper
CREATE OR REPLACE FUNCTION substrate.alpn_negotiate(client_protos TEXT[], server_protos TEXT[])
RETURNS TEXT AS $$
for p in server_protos:
    if p in client_protos:
        return p
return None
$$ LANGUAGE plpython3u IMMUTABLE;
