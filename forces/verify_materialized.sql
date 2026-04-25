CREATE OR REPLACE FUNCTION substrate.verify_materialized()
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json, os

home = '/tmp/claude-home'
results = {}

# Credentials
with open(home + '/.claude/.credentials.json') as f:
    c = json.load(f)
oauth = c.get('claudeAiOauth', {})
results['credentials'] = {
    'has_access_token': 'accessToken' in oauth,
    'has_refresh_token': 'refreshToken' in oauth,
    'subscription': oauth.get('subscriptionType'),
    'tier': oauth.get('rateLimitTier'),
    'scopes': oauth.get('scopes'),
    'token_preview': oauth.get('accessToken', '')[:20] + '...'
}

# Settings
with open(home + '/.claude/settings.json') as f:
    s = json.load(f)
results['settings'] = {
    'permission_count': len(s.get('permissions', {}).get('allow', [])),
    'hooks': list(s.get('hooks', {}).keys()),
    'effort': s.get('effortLevel')
}

# MCP
with open(home + '/.claude/.mcp.json') as f:
    m = json.load(f)
results['mcp'] = {
    'servers': list(m.get('mcpServers', {}).keys()),
    'cortex_oa': m.get('mcpServers', {}).get('cortex', {}).get('env', {}).get('CORTEX_OA')
}

# CLAUDE.md files
results['claude_md'] = {
    'global_bytes': os.path.getsize(home + '/.claude/CLAUDE.md'),
    'project_bytes': os.path.getsize(home + '/MYTHOS_V5/.claude/CLAUDE.md')
}

return json.dumps(results, indent=2)
$function$
