CREATE OR REPLACE FUNCTION substrate.governance_profile(p_level integer)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json, math

level = max(1, min(100, p_level))

# --- RATE LIMIT ---
# 1 = unlimited, 100 = 1/hour
if level <= 5:
    rate_limit = None  # no limit
elif level <= 20:
    rate_limit = '1000/hour'
elif level <= 40:
    rate_limit = '100/hour'
elif level <= 60:
    rate_limit = '50/hour'
elif level <= 80:
    rate_limit = '10/hour'
elif level <= 95:
    rate_limit = '5/hour'
else:
    rate_limit = '1/hour'

# --- GATE ---
# Low = no gates, high = block secrets + large blobs + sensitive compositions
gates = []
if level >= 20:
    gates.append('composition!=secret')
if level >= 50:
    # Max size decreases as governance increases
    # 50 = 500MB, 75 = 100MB, 100 = 10MB
    max_mb = max(10, int(500 - (level - 50) * 9.8))
    gates.append(f'size<{max_mb * 1048576}')
if level >= 70:
    gates.append('composition!=credential')
if level >= 90:
    gates.append('composition!=memory')
gate = ','.join(gates) if gates else None

# --- DEDUP ---
# 1 = no dedup, 100 = 24h window
if level <= 10:
    dedup = None
elif level <= 30:
    dedup = 'PT5M'    # 5 minutes
elif level <= 50:
    dedup = 'PT1H'    # 1 hour
elif level <= 70:
    dedup = 'PT6H'    # 6 hours
elif level <= 90:
    dedup = 'PT12H'   # 12 hours
else:
    dedup = 'P1D'     # 24 hours

# --- COMPRESSION ---
# Low = no compression, high = always compress
if level <= 15:
    compress = None
elif level <= 40:
    compress = 'gzip'   # faster, less compression
else:
    compress = 'zlib'   # slower, better compression

# --- BACKPRESSURE ---
# Low = drop (performance), mid = queue (reliability), high = slow (safety)
if level <= 25:
    backpressure = 'drop'
elif level <= 75:
    backpressure = 'queue'
else:
    backpressure = 'slow'

# --- CIRCUIT BREAKER ---
# Low = tolerant (many failures before trip), high = sensitive (few failures)
if level <= 10:
    cb_max_failures = 100  # basically never trips
elif level <= 30:
    cb_max_failures = 20
elif level <= 50:
    cb_max_failures = 10
elif level <= 70:
    cb_max_failures = 5
elif level <= 90:
    cb_max_failures = 3
else:
    cb_max_failures = 1

# Cooldown: low = short recovery, high = long recovery
cb_cooldown = max(30, int(level * 6))  # 6s to 600s (10 min)

# --- ACK REQUIRED ---
# Below 40 = no ack needed, above = must acknowledge
ack_required = level >= 40

# --- PRIORITY ---
# Inverse of governance: low governance = high priority (fast path)
# High governance = low priority (careful path)
priority = max(1, 100 - level)

# --- MAX QUEUE DEPTH ---
# How many emissions can queue before dropping
if level <= 20:
    max_queue = 10
elif level <= 50:
    max_queue = 100
elif level <= 80:
    max_queue = 1000
else:
    max_queue = 10000

profile = {
    'level': level,
    'rate_limit': rate_limit,
    'gate': gate,
    'dedup': dedup,
    'compress': compress,
    'backpressure': backpressure,
    'cb_max_failures': cb_max_failures,
    'cb_cooldown_seconds': cb_cooldown,
    'ack_required': ack_required,
    'priority': priority,
    'max_queue': max_queue
}

return json.dumps(profile)
$function$
