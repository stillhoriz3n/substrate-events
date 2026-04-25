CREATE OR REPLACE FUNCTION substrate.render(p_open boolean DEFAULT true)
 RETURNS text
 LANGUAGE plpython3u
AS $function$
import json, os, subprocess
from datetime import datetime

# Query the state
rows = plpy.execute("""
    SELECT composition, name, subscriber, hash, internal, retired,
           content_type, sender, recipient, sync_status, origin_peer, ordinal
    FROM substrate.state
    ORDER BY ordinal
""")

# Counts
total = len(rows)
pub_rows = plpy.execute("SELECT count(*) as c FROM substrate.publishable()")
publishable = pub_rows[0]['c']
sig_rows = plpy.execute("SELECT count(*) as c FROM substrate.signal")
signals = sig_rows[0]['c']
msg_rows = plpy.execute("SELECT count(*) as c FROM substrate.state WHERE composition = 'message'")
messages = msg_rows[0]['c']
sync_rows = plpy.execute("SELECT count(*) as c FROM substrate.state WHERE sync_status = 'pending'")
pending = sync_rows[0]['c']

# Group by composition
comps = {}
for r in rows:
    c = r['composition']
    if c not in comps:
        comps[c] = []
    comps[c].append(r)

now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')

html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Substrate — joeys-mac</title>
<meta http-equiv="refresh" content="10">
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ 
    background: #0a0d0c; color: #c4c8c5; 
    font-family: 'SF Mono', 'Menlo', monospace; 
    font-size: 13px; padding: 24px;
}}
h1 {{ color: #82b088; font-size: 18px; margin-bottom: 4px; font-weight: 400; }}
.subtitle {{ color: #5a6e5e; font-size: 12px; margin-bottom: 24px; }}
.stats {{ 
    display: flex; gap: 32px; margin-bottom: 24px; 
    padding: 16px; background: #111714; border: 1px solid #1e2b22; border-radius: 6px;
}}
.stat {{ text-align: center; }}
.stat-val {{ font-size: 28px; color: #82b088; font-weight: 300; }}
.stat-label {{ font-size: 10px; color: #5a6e5e; text-transform: uppercase; letter-spacing: 1px; }}
.section {{ margin-bottom: 20px; }}
.section-header {{ 
    color: #82b088; font-size: 13px; padding: 8px 0; 
    border-bottom: 1px solid #1e2b22; margin-bottom: 8px;
    display: flex; justify-content: space-between;
}}
.blob {{ 
    padding: 6px 12px; border-left: 2px solid #1e2b22;
    margin-bottom: 2px; font-size: 12px;
    display: flex; justify-content: space-between; align-items: center;
}}
.blob:hover {{ background: #111714; }}
.blob-name {{ color: #c4c8c5; }}
.blob-meta {{ color: #3a4e3e; font-size: 11px; }}
.blob-sub {{ color: #3a4e3e; font-size: 10px; }}
.msg {{ border-left-color: #0a84ff; }}
.sync {{ border-left-color: #ff9f0a; }}
.internal {{ border-left-color: #1e2b22; opacity: 0.4; }}
.retired {{ border-left-color: #ff453a; opacity: 0.3; text-decoration: line-through; }}
.laws {{ 
    position: fixed; bottom: 16px; right: 16px; 
    color: #2a3e2e; font-size: 10px; text-align: right; line-height: 1.6;
}}
</style>
</head>
<body>
<h1>substrate — joeys-mac</h1>
<div class="subtitle">10.69.0.60:5433 &middot; {now} &middot; auto-refresh 10s</div>

<div class="stats">
    <div class="stat"><div class="stat-val">{total}</div><div class="stat-label">blobs</div></div>
    <div class="stat"><div class="stat-val">{publishable}</div><div class="stat-label">publishable</div></div>
    <div class="stat"><div class="stat-val">{signals}</div><div class="stat-label">signals</div></div>
    <div class="stat"><div class="stat-val">{messages}</div><div class="stat-label">messages</div></div>
    <div class="stat"><div class="stat-val">{pending}</div><div class="stat-label">sync pending</div></div>
</div>
"""

# Render each composition group
for comp in sorted(comps.keys(), key=lambda c: (-len(comps[c]) if c not in ('field-type','composition') else 999)):
    blobs = comps[comp]
    html += f'<div class="section">'
    html += f'<div class="section-header"><span>{comp}</span><span>{len(blobs)}</span></div>'
    for b in blobs:
        cls = 'blob'
        if b['composition'] == 'message': cls += ' msg'
        elif b['sync_status'] == 'pending': cls += ' sync'
        elif b['internal']: cls += ' internal'
        elif b['retired']: cls += ' retired'
        
        meta_parts = []
        if b['sender']: meta_parts.append(f"from:{b['sender']}")
        if b['recipient']: meta_parts.append(f"to:{b['recipient']}")
        if b['origin_peer']: meta_parts.append(f"via:{b['origin_peer']}")
        if b['sync_status']: meta_parts.append(b['sync_status'])
        meta = ' &middot; '.join(meta_parts)
        
        sub = ','.join(b['subscriber']) if b['subscriber'] else ''
        
        html += f'<div class="{cls}">'
        html += f'<span class="blob-name">{b["name"]}</span>'
        html += f'<span><span class="blob-meta">{meta}</span> <span class="blob-sub">[{sub}]</span></span>'
        html += f'</div>'
    html += '</div>'

html += """
<div class="laws">
    integrity &middot; conservation &middot; causality<br>
    entropy &middot; radiation &middot; gravity &middot; governance
</div>
</body>
</html>
"""

path = '/tmp/substrate-view.html'
with open(path, 'w') as f:
    f.write(html)

if p_open:
    try:
        subprocess.run(['open', path], timeout=5)
    except:
        pass

return path
$function$
