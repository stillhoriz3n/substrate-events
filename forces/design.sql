CREATE OR REPLACE FUNCTION substrate.design(p_text text, p_name text DEFAULT NULL::text, p_mode text DEFAULT 'svg'::text, p_style jsonb DEFAULT NULL::jsonb)
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
import json, os, subprocess, base64, hashlib

# Local variables (don't reassign function args)
name = p_name or ('design-' + hashlib.sha256(p_text.encode()).hexdigest()[:8])

style = p_style if p_style else {}
if isinstance(style, str):
    style = json.loads(style)

if p_mode == 'svg':
    bg = style.get('bg', '#000000')
    fg = style.get('fg', '#ffffff')
    font = style.get('font', 'Inter, system-ui, sans-serif')
    weight = style.get('weight', '900')
    
    words = p_text.split()
    lines = []
    current = []
    target_chars = max(1, int(len(p_text) ** 0.5 * 1.4))
    for w in words:
        if sum(len(x)+1 for x in current) + len(w) > target_chars and current:
            lines.append(' '.join(current))
            current = [w]
        else:
            current.append(w)
    if current:
        lines.append(' '.join(current))
    
    canvas_w, canvas_h = 1200, 1200
    longest = max(len(l) for l in lines) if lines else 1
    font_size = min(canvas_w / (longest * 0.55), canvas_h / (len(lines) * 1.3))
    line_h = font_size * 1.15
    total_h = line_h * len(lines)
    start_y = (canvas_h - total_h) / 2 + font_size * 0.85
    
    text_elems = []
    for i, line in enumerate(lines):
        y = start_y + i * line_h
        text_elems.append(
            f'<text x="600" y="{y:.0f}" font-family="{font}" font-weight="{weight}" '
            f'font-size="{font_size:.0f}" fill="{fg}" text-anchor="middle" '
            f'letter-spacing="-0.02em">{line}</text>'
        )
    
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 1200" '
        f'width="1200" height="1200">'
        f'<rect width="1200" height="1200" fill="{bg}"/>'
        f'{"".join(text_elems)}'
        f'</svg>'
    )
    
    plan = plpy.prepare("""
        INSERT INTO substrate.blob (fields, subscriber)
        VALUES (
            jsonb_build_object(
                'composition', jsonb_build_object('type', 'utf8', 'value', 'image'),
                'name',        jsonb_build_object('type', 'utf8', 'value', $1),
                'content',     jsonb_build_object('type', 'svg', 'value', $2),
                'prompt',      jsonb_build_object('type', 'utf8', 'value', $3),
                'mode',        jsonb_build_object('type', 'utf8', 'value', 'svg'),
                'width',       jsonb_build_object('type', 'integer', 'value', 1200),
                'height',      jsonb_build_object('type', 'integer', 'value', 1200),
                'bytes',       jsonb_build_object('type', 'integer', 'value', $4)
            ),
            ARRAY['SYSTEM']
        ) RETURNING unid
    """, ["text", "text", "text", "int"])
    row = plpy.execute(plan, [name, svg, p_text, len(svg)])
    return row[0]['unid']

elif p_mode == 'dalle':
    token_row = plpy.execute("SELECT current_setting('substrate.openai_token', true) as token")
    token = token_row[0]['token'] if token_row and token_row[0]['token'] else os.environ.get('OPENAI_API_KEY', '')
    if not token:
        plpy.error('No OpenAI token.')
    
    payload = json.dumps({
        'model': 'dall-e-3', 'prompt': p_text, 'n': 1,
        'size': '1024x1024', 'response_format': 'b64_json'
    })
    
    cmd = ['curl', '-sS', '-X', 'POST',
           'https://api.openai.com/v1/images/generations',
           '-H', f'Authorization: Bearer {token}',
           '-H', 'Content-Type: application/json',
           '-d', payload]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    
    try:
        resp = json.loads(r.stdout)
        b64 = resp['data'][0]['b64_json']
    except Exception as e:
        plpy.error(f'DALL-E failed: {r.stdout[:500]}')
    
    plan = plpy.prepare("""
        INSERT INTO substrate.blob (fields, subscriber)
        VALUES (
            jsonb_build_object(
                'composition', jsonb_build_object('type', 'utf8', 'value', 'image'),
                'name',        jsonb_build_object('type', 'utf8', 'value', $1),
                'content',     jsonb_build_object('type', 'png', 'value', $2),
                'prompt',      jsonb_build_object('type', 'utf8', 'value', $3),
                'mode',        jsonb_build_object('type', 'utf8', 'value', 'dalle'),
                'width',       jsonb_build_object('type', 'integer', 'value', 1024),
                'height',      jsonb_build_object('type', 'integer', 'value', 1024),
                'bytes',       jsonb_build_object('type', 'integer', 'value', $4)
            ),
            ARRAY['SYSTEM']
        ) RETURNING unid
    """, ["text", "text", "text", "int"])
    row = plpy.execute(plan, [name, b64, p_text, len(b64)])
    return row[0]['unid']

else:
    plpy.error(f'Unknown mode: {p_mode}')
$function$
