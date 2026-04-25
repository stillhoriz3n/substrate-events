CREATE OR REPLACE FUNCTION substrate.install(p_url text, p_dest_path text, p_name text DEFAULT NULL::text)
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
import subprocess, os

tmp = p_dest_path + '.download'
kwargs = dict(stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE, text=True, shell=True)
if os.name == 'nt':
    kwargs['creationflags'] = 0x08000000
    dl_cmd = f'powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri \'{p_url}\' -OutFile \'{tmp}\' -UseBasicParsing"'
else:
    dl_cmd = f'curl -sL -o {tmp} "{p_url}"'

plpy.notice(f'Downloading {p_url}...')
result = subprocess.run(dl_cmd, **kwargs)

if not os.path.exists(tmp):
    plpy.error(f'Download failed: {result.stderr}')

# If it's a zip, extract the main binary
if tmp.endswith('.zip.download') or p_url.endswith('.zip'):
    import zipfile
    extract_dir = tmp + '_extracted'
    with zipfile.ZipFile(tmp, 'r') as z:
        z.extractall(extract_dir)
    os.remove(tmp)
    # Find the main executable
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f.endswith('.exe') or (not '.' in f and os.access(os.path.join(root, f), os.X_OK)):
                import shutil
                shutil.move(os.path.join(root, f), p_dest_path)
                break
    import shutil
    shutil.rmtree(extract_dir, ignore_errors=True)
else:
    os.rename(tmp, p_dest_path)

try:
    os.chmod(p_dest_path, 0o755)
except:
    pass

# Ingest into Substrate
row = plpy.execute(f"SELECT substrate.ingest('{p_dest_path}')")
return row[0]['ingest']
$function$
