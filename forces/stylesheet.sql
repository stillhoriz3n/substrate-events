CREATE OR REPLACE FUNCTION substrate.stylesheet(p_name text DEFAULT 'apple-design-system'::text)
 RETURNS text
 LANGUAGE plpython3u
AS $function$
import json
row = plpy.execute(plpy.prepare("""
    SELECT fields->'content'->>'value' as css FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'stylesheet'
    AND fields->'name'->>'value' = $1 LIMIT 1
""", ["text"]), [p_name])
return row[0]['css'] if row else ''
$function$
