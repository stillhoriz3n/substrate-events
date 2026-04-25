CREATE OR REPLACE FUNCTION substrate.render_designs()
 RETURNS text
 LANGUAGE plpython3u
AS $function$
import json

css = plpy.execute("SELECT substrate.stylesheet('apple-design-system') as css")[0]['css']

rows = plpy.execute("""
    SELECT unid,
           fields->'name'->>'value' as name,
           fields->'prompt'->>'value' as prompt,
           fields->'content'->>'value' as svg,
           fields->'mode'->>'value' as mode,
           fields->'bytes'->>'value' as bytes
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'image'
    ORDER BY ordinal
""")

cards = []
for r in rows:
    if r['mode'] == 'svg':
        preview = r['svg']
    else:
        preview = f'<img src="data:image/png;base64,{r["svg"]}" style="width:100%;height:100%;object-fit:cover" />'
    cards.append(f'''
        <div class="card glass">
          <div class="canvas">{preview}</div>
          <div class="meta">
            <div class="eyebrow">{r["name"]}</div>
            <div class="prompt">{r["prompt"]}</div>
          </div>
        </div>
    ''')

html = f'''<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>Substrate Merch</title>
<style>{css}</style>
<style>
  .container {{ max-width: 1400px; margin: 0 auto; padding: 64px 32px 96px; }}
  header {{ display: flex; align-items: end; justify-content: space-between; margin-bottom: 48px; }}
  .title-stack h1 {{ font-size: 40px; letter-spacing: -0.03em; }}
  .title-stack .sub {{ color: var(--text-secondary); margin-top: 6px; font-size: 15px; }}
  .pill {{ display:inline-flex; align-items:center; gap:8px; padding:8px 14px; border-radius:999px;
          background: var(--bg-elev-2); border: 0.5px solid var(--hairline); font-size: 13px; }}
  .grid {{ display:grid; grid-template-columns:repeat(auto-fill, minmax(300px, 1fr)); gap: 16px; }}
  .card {{ overflow: hidden; transition: transform 0.3s var(--easing); }}
  .card:hover {{ transform: translateY(-2px); }}
  .canvas {{ aspect-ratio:1; background:#000; display:flex; align-items:center; justify-content:center; overflow:hidden;
             border-bottom: 0.5px solid var(--hairline); }}
  .canvas svg {{ width:100%; height:100% }}
  .meta {{ padding: 16px 18px 18px; }}
  .prompt {{ color: var(--text); font-size: 14px; line-height: 1.4; margin-top: 6px; font-weight: 500; }}
</style></head><body>
<div class="container">
  <header>
    <div class="title-stack">
      <h1>Substrate Merch</h1>
      <div class="sub">{len(rows)} designs · all blobs · composition: image</div>
    </div>
    <div class="pill"><span class="dot green"></span> live from substrate.blob</div>
  </header>
  <div class="grid">{"".join(cards)}</div>
</div>
</body></html>'''

with open('/tmp/substrate-merch.html', 'w') as f:
    f.write(html)
return '/tmp/substrate-merch.html'
$function$
