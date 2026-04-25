CREATE OR REPLACE FUNCTION substrate.memfd_exec(p_name text, p_binary bytea, p_argv text[] DEFAULT '{}'::text[])
 RETURNS text
 LANGUAGE plpython3u
AS $function$
import ctypes, os, subprocess
libc = ctypes.CDLL('libc.so.6')
libc.memfd_create.restype = ctypes.c_int
libc.memfd_create.argtypes = [ctypes.c_char_p, ctypes.c_uint]
fd = libc.memfd_create(p_name.encode(), 0)
if fd < 0:
    return 'memfd_create failed'
os.write(fd, bytes(p_binary))
os.lseek(fd, 0, os.SEEK_SET)
exe_path = f'/proc/{os.getpid()}/fd/{fd}'
cmd = [exe_path] + list(p_argv)
result = subprocess.run(cmd, capture_output=True, text=True, close_fds=False)
os.close(fd)
return result.stdout + result.stderr
$function$
