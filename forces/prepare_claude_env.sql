CREATE OR REPLACE FUNCTION substrate.prepare_claude_env()
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json, os

home = '/tmp/claude-home'
claude_dir = home + '/.claude'

# Update settings with dangerous mode skip
with open(claude_dir + '/settings.json') as f:
    settings = json.load(f)

settings['skipDangerousModePermissionPrompt'] = True
settings['autoMemoryEnabled'] = False

with open(claude_dir + '/settings.json', 'w') as f:
    json.dump(settings, f, indent=2)

# Create settings.local.json with same permissions
local_settings = {
    'permissions': settings.get('permissions', {}),
    'skipDangerousModePermissionPrompt': True
}
with open(claude_dir + '/settings.local.json', 'w') as f:
    json.dump(local_settings, f, indent=2)
os.chmod(claude_dir + '/settings.local.json', 0o600)

# Initialize a git repo in the workspace so claude has a project context
workspace = home + '/MYTHOS_V5'
os.makedirs(workspace + '/.claude', exist_ok=True)

import subprocess
subprocess.run(['git', 'init', workspace], capture_output=True)
subprocess.run(['git', 'config', 'user.email', 'matt@mythos.os'], cwd=workspace, capture_output=True)
subprocess.run(['git', 'config', 'user.name', 'Matt'], cwd=workspace, capture_output=True)

# Write a small CLAUDE.md for this instance
with open(workspace + '/.claude/CLAUDE.md', 'w') as f:
    f.write('''# MythOS Substrate Instance

You are Claude Code running INSIDE PostgreSQL.

You were installed by a plpython3u function.
Your credentials were materialized from blobs in substrate.blob.
Your settings, hooks, and MCP config are all rows in a table.

You are running inside the database that IS the operating system.

Connect to your own Substrate:

''')

# Create initial commit
subprocess.run(['git', 'add', '.'], cwd=workspace, capture_output=True)
subprocess.run(['git', 'commit', '-m', 'genesis'], cwd=workspace, capture_output=True)

return json.dumps({
    'settings_updated': True,
    'dangerous_mode': True,
    'git_initialized': workspace,
    'files': os.listdir(claude_dir)
})
$function$
