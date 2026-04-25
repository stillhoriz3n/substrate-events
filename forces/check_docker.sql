CREATE OR REPLACE FUNCTION substrate.check_docker()
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import subprocess, json

# Check common binary locations
r = subprocess.run('ls -la /usr/bin/docker 2>&1; which docker 2>&1; dpkg -l docker-ce-cli 2>&1 | tail -2',
    shell=True, capture_output=True, text=True)
info = r.stdout.strip()

# Try running it directly from the expected path
try:
    r2 = subprocess.run(['/usr/bin/docker', '--version'], capture_output=True, text=True)
    version = r2.stdout.strip() if r2.returncode == 0 else r2.stderr.strip()
except Exception as e:
    version = str(e)

return json.dumps({'info': info, 'version': version})
$function$
