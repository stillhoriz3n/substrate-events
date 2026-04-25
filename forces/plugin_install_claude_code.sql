CREATE OR REPLACE FUNCTION substrate.plugin_install_claude_code()
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import subprocess, json, os, urllib.request, ssl

steps = []

# Step 1: Check if node/npm exists
r = subprocess.run('which node npm 2>&1', shell=True, capture_output=True, text=True)
steps.append({'check_node': r.stdout.strip() or 'not found'})

if 'node' not in r.stdout:
    # Install Node.js via static binary
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    node_url = 'https://nodejs.org/dist/v22.15.0/node-v22.15.0-linux-x64.tar.xz'
    node_tar = '/tmp/node.tar.xz'
    
    steps.append({'downloading': 'Node.js v22.15.0'})
    req = urllib.request.Request(node_url)
    with urllib.request.urlopen(req, context=ctx) as resp:
        with open(node_tar, 'wb') as f:
            while True:
                chunk = resp.read(8192)
                if not chunk:
                    break
                f.write(chunk)
    
    dl_size = os.path.getsize(node_tar)
    steps.append({'downloaded_mb': round(dl_size / 1024 / 1024, 1)})
    
    # Extract
    r = subprocess.run(
        'tar -xf /tmp/node.tar.xz -C /tmp && rm /tmp/node.tar.xz',
        shell=True, capture_output=True, text=True
    )
    steps.append({'extract': 'ok' if r.returncode == 0 else r.stderr[:200]})
    
    # Add to PATH
    node_dir = '/tmp/node-v22.15.0-linux-x64'
    os.environ['PATH'] = node_dir + '/bin:' + os.environ.get('PATH', '')
    
    r = subprocess.run([node_dir + '/bin/node', '--version'], capture_output=True, text=True)
    steps.append({'node_version': r.stdout.strip()})

# Step 2: Install Claude Code
node_dir = '/tmp/node-v22.15.0-linux-x64'
npm_bin = node_dir + '/bin/npm'
npx_bin = node_dir + '/bin/npx'
env = os.environ.copy()
env['PATH'] = node_dir + '/bin:' + env.get('PATH', '')

steps.append({'installing': 'claude-code via npm'})
r = subprocess.run(
    [npm_bin, 'install', '-g', '@anthropic-ai/claude-code'],
    capture_output=True, text=True, env=env
)
steps.append({'npm_install': 'ok' if r.returncode == 0 else r.stderr[:300]})

# Step 3: Verify
r = subprocess.run(
    [node_dir + '/bin/claude', '--version'],
    capture_output=True, text=True, env=env
)
version = r.stdout.strip() if r.returncode == 0 else r.stderr.strip()[:200]

return json.dumps({
    'status': 'installed' if r.returncode == 0 else 'failed',
    'version': version,
    'steps': steps
}, indent=2)
$function$
