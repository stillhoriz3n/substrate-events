CREATE OR REPLACE FUNCTION substrate.exec(p_file_unid uuid, p_argv text[] DEFAULT '{}'::text[], p_env jsonb DEFAULT '{}'::jsonb)
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
import subprocess, os, json

row = plpy.execute(plpy.prepare(
    "SELECT fields->'path'->>'value' as path FROM substrate.blob WHERE unid = $1",
    ["uuid"]
), [p_file_unid])

if not row:
    plpy.error(f'file blob {p_file_unid} not found')

path = row[0]['path']

if not os.path.exists(path):
    plpy.execute(plpy.prepare("SELECT substrate.materialize($1)", ["uuid"]), [p_file_unid])

cmd = [path] + list(p_argv) if p_argv else [path]

env = dict(os.environ)
if p_env and str(p_env) != '{}':
    env_extra = json.loads(str(p_env)) if isinstance(p_env, str) else dict(p_env)
    env.update(env_extra)

try:
    kwargs = dict(stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE, text=True)
    if os.name == 'nt':
        kwargs['creationflags'] = 0x08000000
    result = subprocess.run(cmd, env=env, **kwargs)
    stdout_val = result.stdout or ''
    stderr_val = result.stderr or ''
    exitcode = result.returncode
except Exception as e:
    stdout_val = ''
    stderr_val = str(e)
    exitcode = -1

plan = plpy.prepare("""
    INSERT INTO substrate.blob (fields, subscriber)
    VALUES (
        jsonb_build_object(
            'composition', jsonb_build_object('type', 'utf8', 'value', 'process'),
            'name',        jsonb_build_object('type', 'utf8', 'value', $1),
            'executable',  jsonb_build_object('type', 'reference', 'value', $2),
            'argv',        jsonb_build_object('type', 'argv', 'value', to_jsonb($3::text[])),
            'stdout',      jsonb_build_object('type', 'utf8', 'value', $4),
            'stderr',      jsonb_build_object('type', 'utf8', 'value', $5),
            'exitcode',    jsonb_build_object('type', 'exitcode', 'value', $6),
            'state',       jsonb_build_object('type', 'utf8', 'value', 'completed')
        ),
        '{SYSTEM}'
    ) RETURNING unid
""", ["text", "text", "text[]", "text", "text", "int"])

proc_row = plpy.execute(plan, [
    os.path.basename(path),
    str(p_file_unid),
    list(p_argv) if p_argv else [],
    stdout_val[:100000],
    stderr_val[:100000],
    exitcode
])
proc_unid = proc_row[0]['unid']

sig_plan = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'exec', jsonb_build_object('file', $2, 'exitcode', $3),
            '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text", "int"])
plpy.execute(sig_plan, [proc_unid, str(p_file_unid), exitcode])

return proc_unid
$function$
