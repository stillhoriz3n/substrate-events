CREATE OR REPLACE FUNCTION substrate.install(p_url text, p_dest_path text)
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
import subprocess, os, shutil

tmp = p_dest_path + '.download'
os.makedirs(os.path.dirname(p_dest_path) or '/tmp', exist_ok=True)

plpy.notice(f'Downloading {p_url}...')
subprocess.run(f'curl -sL -o "{tmp}" "{p_url}"', shell=True, capture_output=True)

if not os.path.exists(tmp):
    plpy.error('Download failed')

if p_url.endswith('.tar.xz') or p_url.endswith('.tar.gz'):
    extract_dir = tmp + '_ex'
    os.makedirs(extract_dir, exist_ok=True)
    subprocess.run(f'tar xf "{tmp}" -C "{extract_dir}"', shell=True, capture_output=True)
    os.remove(tmp)
    # Find the main binary
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f == 'node' or f == 'npm' or f == 'npx':
                fp = os.path.join(root, f)
                dest = os.path.join(os.path.dirname(p_dest_path), f)
                shutil.copy2(fp, dest)
                os.chmod(dest, 0o755)
    shutil.rmtree(extract_dir, ignore_errors=True)
elif p_url.endswith('.zip'):
    import zipfile
    extract_dir = tmp + '_ex'
    with zipfile.ZipFile(tmp, 'r') as z:
        z.extractall(extract_dir)
    os.remove(tmp)
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f.endswith('.exe') or (not '.' in f and os.access(os.path.join(root, f), os.X_OK)):
                shutil.move(os.path.join(root, f), p_dest_path)
                break
    shutil.rmtree(extract_dir, ignore_errors=True)
else:
    os.rename(tmp, p_dest_path)
    os.chmod(p_dest_path, 0o755)

plan = plpy.prepare("SELECT substrate.ingest($1)", ["text"])
row = plpy.execute(plan, [p_dest_path])
return row[0]['ingest']
$function$
