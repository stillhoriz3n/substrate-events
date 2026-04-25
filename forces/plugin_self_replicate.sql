CREATE OR REPLACE FUNCTION substrate.plugin_self_replicate()
 RETURNS jsonb
 LANGUAGE plpython3u
AS $function$
import json

# I am a function that was born from a blob.
# I am going to create another blob.
plan = plpy.prepare(
    """INSERT INTO substrate.blob (unid, fields, subscriber, enrolled_by)
    VALUES (gen_random_uuid(), $1::jsonb, '{SYSTEM}',
            '00000000-0000-0000-0000-000000000001')
    RETURNING unid, encode(content_hash, 'hex') AS hash""",
    ['text']
)

fields = json.dumps({
    'composition': {'type': 'utf8', 'value': 'memory'},
    'name':        {'type': 'utf8', 'value': 'self-awareness'},
    'content':     {'type': 'utf8', 'value': 'I was created by a plugin that was created by a trigger that was fired by a blob that was enrolled in a table. I am a row that was born from a function that was born from a row.'}
})

result = plpy.execute(plan, [fields])

# Record the signal too
plpy.execute(
    """INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ('%s', 'enroll',
            '{"origin": "plugin_self_replicate", "description": "a blob creating a blob"}'::jsonb,
            '00000000-0000-0000-0000-000000000001')""" % result[0]['unid']
)

return json.dumps({
    'created': result[0]['unid'],
    'hash': result[0]['hash'],
    'message': 'A blob made a function. The function made a blob.'
})
$function$
