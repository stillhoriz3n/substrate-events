CREATE OR REPLACE FUNCTION substrate.gate(p_blob_unid uuid, p_gate_expr text)
 RETURNS boolean
 LANGUAGE plpython3u
AS $function$
import json

if not p_gate_expr or p_gate_expr == '*':
    return True

row = plpy.execute(plpy.prepare(
    "SELECT fields FROM substrate.blob WHERE unid = $1", ["uuid"]
), [p_blob_unid])

if not row:
    return False

fields = row[0]['fields']
if isinstance(fields, str):
    fields = json.loads(fields)

# Parse gate expressions: "composition!=secret", "size<10485760", "composition=file"
# Multiple conditions separated by comma: "composition!=secret,size<104857600"
conditions = [c.strip() for c in p_gate_expr.split(',')]

for cond in conditions:
    if '!=' in cond:
        key, val = cond.split('!=', 1)
        blob_val = str(fields.get(key, {}).get('value', ''))
        if blob_val == val:
            return False
    elif '<=' in cond:
        key, val = cond.split('<=', 1)
        blob_val = fields.get(key, {}).get('value', 0)
        if int(blob_val) > int(val):
            return False
    elif '>=' in cond:
        key, val = cond.split('>=', 1)
        blob_val = fields.get(key, {}).get('value', 0)
        if int(blob_val) < int(val):
            return False
    elif '<' in cond:
        key, val = cond.split('<', 1)
        blob_val = fields.get(key, {}).get('value', 0)
        if int(blob_val) >= int(val):
            return False
    elif '>' in cond:
        key, val = cond.split('>', 1)
        blob_val = fields.get(key, {}).get('value', 0)
        if int(blob_val) <= int(val):
            return False
    elif '=' in cond:
        key, val = cond.split('=', 1)
        blob_val = str(fields.get(key, {}).get('value', ''))
        if blob_val != val:
            return False

return True
$function$
