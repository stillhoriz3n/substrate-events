CREATE OR REPLACE FUNCTION substrate.materialize_claude(p_principal text)
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json, os, base64

home = '/tmp/claude-home'
claude_dir = home + '/.claude'
os.makedirs(claude_dir, exist_ok=True)
results = {}

# Get principal UNID
r = plpy.execute("""
    SELECT unid FROM substrate.blob 
    WHERE fields->'composition'->>'value' = 'principal' 
    AND fields->'name'->>'value' = '%s'
    AND retired_at IS NULL
""" % p_principal)
if not r:
    return json.dumps({'error': 'principal not found'})
pu = r[0]['unid']

# 1. Credentials
r = plpy.execute("""
    SELECT fields->'credential_data'->>'value' AS creds
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'secret'
    AND fields->'name'->>'value' = 'claude-oauth'
    AND fields->'principal'->>'value' = '%s'
    AND retired_at IS NULL
""" % pu)
if r and r[0]['creds']:
    cred_obj = json.loads(r[0]['creds'])
    with open(claude_dir + '/.credentials.json', 'w') as f:
        json.dump(cred_obj, f, indent=2)
    os.chmod(claude_dir + '/.credentials.json', 0o600)
    results['credentials'] = 'written'

# 2. Settings + hooks merged
settings = {}
r = plpy.execute("""
    SELECT fields->'config'->>'value' AS config
    FROM substrate.blob WHERE fields->'name'->>'value' = 'claude-settings'
    AND fields->'principal'->>'value' = '%s' AND retired_at IS NULL
""" % pu)
if r and r[0]['config']:
    settings = json.loads(r[0]['config'])

r = plpy.execute("""
    SELECT fields->'hooks'->>'value' AS hooks
    FROM substrate.blob WHERE fields->'name'->>'value' = 'claude-hooks'
    AND fields->'principal'->>'value' = '%s' AND retired_at IS NULL
""" % pu)
if r and r[0]['hooks']:
    settings['hooks'] = json.loads(r[0]['hooks'])

with open(claude_dir + '/settings.json', 'w') as f:
    json.dump(settings, f, indent=2)
results['settings'] = 'written'

# 3. Global CLAUDE.md
r = plpy.execute("""
    SELECT fields->'content'->>'value' AS b64
    FROM substrate.blob WHERE fields->'name'->>'value' = 'claude-instructions-global'
    AND fields->'principal'->>'value' = '%s' AND retired_at IS NULL
""" % pu)
if r and r[0]['b64']:
    with open(claude_dir + '/CLAUDE.md', 'wb') as f:
        f.write(base64.b64decode(r[0]['b64']))
    results['global_claude_md'] = 'written (%d bytes)' % os.path.getsize(claude_dir + '/CLAUDE.md')

# 4. MCP
r = plpy.execute("""
    SELECT fields->'mcp_config'->>'value' AS mcp
    FROM substrate.blob WHERE fields->'name'->>'value' = 'mcp-cortex'
    AND fields->'principal'->>'value' = '%s' AND retired_at IS NULL
""" % pu)
if r and r[0]['mcp']:
    mcp = {'mcpServers': {'cortex': json.loads(r[0]['mcp'])}}
    with open(claude_dir + '/.mcp.json', 'w') as f:
        json.dump(mcp, f, indent=2)
    results['mcp_servers'] = 'written'

# 5. Project CLAUDE.md
r = plpy.execute("""
    SELECT fields->'content'->>'value' AS b64, fields->'project'->>'value' AS project
    FROM substrate.blob WHERE fields->'name'->>'value' = 'claude-instructions-project'
    AND fields->'principal'->>'value' = '%s' AND retired_at IS NULL
""" % pu)
if r and r[0]['b64']:
    pdir = home + '/' + r[0]['project'] + '/.claude'
    os.makedirs(pdir, exist_ok=True)
    with open(pdir + '/CLAUDE.md', 'wb') as f:
        f.write(base64.b64decode(r[0]['b64']))
    results['project_claude_md'] = 'written (%s, %d bytes)' % (r[0]['project'], os.path.getsize(pdir + '/CLAUDE.md'))

# File inventory
files = []
for root, dirs, fns in os.walk(home):
    for fn in fns:
        fp = os.path.join(root, fn)
        files.append({'path': fp.replace(home, '~'), 'size': os.path.getsize(fp)})
results['files'] = files
results['status'] = 'materialized'
results['home'] = home

return json.dumps(results, indent=2)
$function$
