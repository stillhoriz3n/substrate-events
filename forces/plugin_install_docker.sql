CREATE OR REPLACE FUNCTION substrate.plugin_install_docker()
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import subprocess, json, urllib.request, tarfile, os, ssl

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url = 'https://download.docker.com/linux/static/stable/x86_64/docker-24.0.7.tgz'
tgz_path = '/tmp/docker.tgz'
docker_bin = '/tmp/docker-cli'

req = urllib.request.Request(url)
with urllib.request.urlopen(req, context=ctx) as resp:
    with open(tgz_path, 'wb') as f:
        while True:
            chunk = resp.read(8192)
            if not chunk:
                break
            f.write(chunk)

size = os.path.getsize(tgz_path)

with tarfile.open(tgz_path, 'r:gz') as tar:
    member = tar.getmember('docker/docker')
    ef = tar.extractfile(member)
    with open(docker_bin, 'wb') as out:
        out.write(ef.read())

os.chmod(docker_bin, 0o755)
os.remove(tgz_path)

r = subprocess.run([docker_bin, '--version'], capture_output=True, text=True)

return json.dumps({
    'status': 'installed',
    'version': r.stdout.strip(),
    'path': docker_bin,
    'download_mb': round(size / 1024 / 1024, 1),
    'binary_mb': round(os.path.getsize(docker_bin) / 1024 / 1024, 1)
})
$function$
