-- ============================================================
-- SUBSTRATE LIBRARY: lib.time
-- Time conversions, duration parsing, cron, calendrics
-- ============================================================

-- Unix epoch to ISO 8601
CREATE OR REPLACE FUNCTION substrate.epoch_to_iso(epoch FLOAT8)
RETURNS TEXT AS $$
from datetime import datetime, timezone
return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
$$ LANGUAGE plpython3u IMMUTABLE;

-- ISO 8601 to unix epoch
CREATE OR REPLACE FUNCTION substrate.iso_to_epoch(iso TEXT)
RETURNS FLOAT8 AS $$
from datetime import datetime, timezone
dt = datetime.fromisoformat(iso.replace('Z', '+00:00'))
return dt.timestamp()
$$ LANGUAGE plpython3u IMMUTABLE;

-- Parse human duration string to seconds: "1h30m", "2d 4h", "500ms"
CREATE OR REPLACE FUNCTION substrate.parse_duration(input TEXT)
RETURNS FLOAT8 AS $$
import re
total = 0
for val, unit in re.findall(r'([\d.]+)\s*(ms|s|m|h|d|w)', input.lower()):
    v = float(val)
    if unit == 'ms': total += v / 1000
    elif unit == 's': total += v
    elif unit == 'm': total += v * 60
    elif unit == 'h': total += v * 3600
    elif unit == 'd': total += v * 86400
    elif unit == 'w': total += v * 604800
return total
$$ LANGUAGE plpython3u IMMUTABLE;

-- Seconds to human duration
CREATE OR REPLACE FUNCTION substrate.human_duration(total_secs FLOAT8)
RETURNS TEXT AS $$
s = total_secs
if s < 0.001: return '0s'
if s < 1: return f'{s*1000:.0f}ms'
parts = []
for unit, div in [('d',86400),('h',3600),('m',60)]:
    if s >= div:
        n = int(s // div)
        s -= n * div
        parts.append(f'{n}{unit}')
if s > 0.5 or not parts:
    parts.append(f'{s:.0f}s')
return ' '.join(parts)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Day of week (0=Monday) from date
CREATE OR REPLACE FUNCTION substrate.day_of_week(d DATE)
RETURNS INT AS $$ SELECT EXTRACT(ISODOW FROM d)::int - 1 $$ LANGUAGE sql IMMUTABLE;

-- Is weekend
CREATE OR REPLACE FUNCTION substrate.is_weekend(d DATE)
RETURNS BOOLEAN AS $$ SELECT EXTRACT(ISODOW FROM d)::int >= 6 $$ LANGUAGE sql IMMUTABLE;

-- Business days between two dates (Mon-Fri)
CREATE OR REPLACE FUNCTION substrate.business_days(start_date DATE, end_date DATE)
RETURNS INT AS $$
from datetime import timedelta
d = start_date
count = 0
one_day = timedelta(days=1)
while d < end_date:
    if d.weekday() < 5:
        count += 1
    d += one_day
return count
$$ LANGUAGE plpython3u IMMUTABLE;

-- Next cron occurrence from now (simple: supports "m h dom mon dow")
CREATE OR REPLACE FUNCTION substrate.next_cron(cron_expr TEXT, from_ts TIMESTAMPTZ DEFAULT now())
RETURNS TIMESTAMPTZ AS $$
from datetime import datetime, timedelta, timezone

parts = cron_expr.strip().split()
if len(parts) != 5: return None

def parse_field(field, mn, mx):
    if field == '*': return list(range(mn, mx+1))
    vals = set()
    for part in field.split(','):
        if '/' in part:
            base, step = part.split('/')
            start = mn if base == '*' else int(base)
            vals.update(range(start, mx+1, int(step)))
        elif '-' in part:
            a, b = part.split('-')
            vals.update(range(int(a), int(b)+1))
        else:
            vals.add(int(part))
    return sorted(vals)

minutes = parse_field(parts[0], 0, 59)
hours = parse_field(parts[1], 0, 23)
doms = parse_field(parts[2], 1, 31)
months = parse_field(parts[3], 1, 12)
dows = parse_field(parts[4], 0, 6)

dt = from_ts.replace(second=0, microsecond=0) + timedelta(minutes=1)
for _ in range(525960):  # max 1 year of minutes
    if (dt.month in months and dt.day in doms and
        dt.weekday() in [d % 7 for d in dows] and
        dt.hour in hours and dt.minute in minutes):
        return dt
    dt += timedelta(minutes=1)
return None
$$ LANGUAGE plpython3u;

-- Time ago (human-readable relative time)
CREATE OR REPLACE FUNCTION substrate.time_ago(input_ts TIMESTAMPTZ)
RETURNS TEXT AS $$
plan = plpy.prepare("SELECT EXTRACT(EPOCH FROM now() - $1::timestamptz) AS secs", ["text"])
result = plpy.execute(plan, [str(input_ts)])
secs = float(result[0]['secs'])
if secs < 60: return f'{int(secs)}s ago'
if secs < 3600: return f'{int(secs//60)}m ago'
if secs < 86400: return f'{int(secs//3600)}h ago'
if secs < 604800: return f'{int(secs//86400)}d ago'
if secs < 2592000: return f'{int(secs//604800)}w ago'
return f'{int(secs//2592000)}mo ago'
$$ LANGUAGE plpython3u;

-- Timezone conversion
CREATE OR REPLACE FUNCTION substrate.tz_convert(ts TIMESTAMPTZ, to_tz TEXT)
RETURNS TEXT AS $$ SELECT (ts AT TIME ZONE to_tz)::text $$ LANGUAGE sql IMMUTABLE;
