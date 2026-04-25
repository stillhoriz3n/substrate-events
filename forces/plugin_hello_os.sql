CREATE OR REPLACE FUNCTION substrate.plugin_hello_os()
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import subprocess, json, os
hostname = subprocess.check_output(['hostname']).decode().strip()
uname = subprocess.check_output(['uname', '-a']).decode().strip()
return json.dumps({
    'message': 'I am a row in a table that became a function that talked to the OS',
    'hostname': hostname,
    'kernel': uname,
    'pid': os.getpid()
})
$function$
