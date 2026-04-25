CREATE OR REPLACE FUNCTION substrate.invoke(p_persona text, p_input text, p_model text DEFAULT 'claude-sonnet-4-20250514'::text)
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
import json

# Look up the persona blob
rows = plpy.execute(plpy.prepare("""
    SELECT unid, fields FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'persona'
    AND fields->'name'->>'value' = $1
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
    LIMIT 1
""", ['text']), [p_persona])

if not rows:
    plpy.error(f'persona "{p_persona}" not found')

persona_fields = rows[0]['fields']
if isinstance(persona_fields, str):
    persona_fields = json.loads(persona_fields)

system_prompt = persona_fields.get('body', {}).get('value', '')
persona_unid = rows[0]['unid']

# Get API key
token_row = plpy.execute("SELECT current_setting('substrate.anthropic_key', true) as key")
api_key = token_row[0]['key'] if token_row and token_row[0]['key'] else None

if not api_key:
    # Store the invocation request as a memory blob for later processing
    result_fields = {
        'composition': {'type': 'utf8', 'value': 'memory'},
        'name': {'type': 'utf8', 'value': f'invoke-{p_persona}-pending'},
        'persona': {'type': 'reference', 'value': str(persona_unid)},
        'persona_name': {'type': 'utf8', 'value': p_persona},
        'input': {'type': 'utf8', 'value': p_input},
        'system_prompt': {'type': 'utf8', 'value': system_prompt},
        'model': {'type': 'utf8', 'value': p_model},
        'status': {'type': 'utf8', 'value': 'pending-no-key'},
        'body': {'type': 'utf8', 'value': f'Invocation of {p_persona} queued. Set substrate.anthropic_key to process.'}
    }
    plan = plpy.prepare("""
        INSERT INTO substrate.blob (fields, subscriber)
        VALUES ($1::jsonb, ARRAY['joey', 'SYSTEM'])
        RETURNING unid
    """, ['text'])
    row = plpy.execute(plan, [json.dumps(result_fields)])
    return row[0]['unid']

# Call Anthropic API
import urllib.request

payload = json.dumps({
    'model': p_model,
    'max_tokens': 8192,
    'system': system_prompt,
    'messages': [{'role': 'user', 'content': p_input}]
}).encode('utf-8')

req = urllib.request.Request(
    'https://api.anthropic.com/v1/messages',
    data=payload,
    headers={
        'Content-Type': 'application/json',
        'x-api-key': api_key,
        'anthropic-version': '2023-06-01'
    }
)

try:
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read().decode('utf-8'))
    
    response_text = result.get('content', [{}])[0].get('text', '')
    
    # Store response as a blob
    result_fields = {
        'composition': {'type': 'utf8', 'value': 'memory'},
        'name': {'type': 'utf8', 'value': f'invoke-{p_persona}-response'},
        'persona': {'type': 'reference', 'value': str(persona_unid)},
        'persona_name': {'type': 'utf8', 'value': p_persona},
        'input': {'type': 'utf8', 'value': p_input[:500]},
        'model': {'type': 'utf8', 'value': p_model},
        'body': {'type': 'utf8', 'value': response_text}
    }
    
    plan = plpy.prepare("""
        INSERT INTO substrate.blob (fields, subscriber)
        VALUES ($1::jsonb, ARRAY['joey', 'SYSTEM'])
        RETURNING unid
    """, ['text'])
    row = plpy.execute(plan, [json.dumps(result_fields)])
    
    # Signal
    sig = plpy.prepare("""
        INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
        VALUES ($1, 'invoke', jsonb_build_object('persona', $2, 'model', $3),
                '00000000-0000-0000-0000-000000000001')
    """, ['uuid', 'text', 'text'])
    plpy.execute(sig, [row[0]['unid'], p_persona, p_model])
    
    return row[0]['unid']

except Exception as e:
    plpy.error(f'invoke failed: {str(e)[:200]}')

$function$
