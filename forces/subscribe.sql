CREATE OR REPLACE FUNCTION substrate.subscribe(p_subscriber_name text, p_target_filter text, p_endpoint text, p_protocol text DEFAULT 'pg'::text, p_frequency text DEFAULT 'on_change'::text, p_compress text DEFAULT NULL::text)
 RETURNS uuid
 LANGUAGE plpython3u
AS $function$
import json

fields = {
    'composition': {'type': 'utf8', 'value': 'subscription'},
    'subscriber':  {'type': 'utf8', 'value': p_subscriber_name},
    'target':      {'type': 'filter', 'value': p_target_filter},
    'endpoint':    {'type': 'endpoint', 'value': p_endpoint},
    'protocol':    {'type': 'protocol', 'value': p_protocol},
    'frequency':   {'type': 'frequency', 'value': p_frequency},
}

if p_compress:
    fields['compress'] = {'type': 'algorithm', 'value': p_compress}

plan = plpy.prepare("""
    INSERT INTO substrate.blob (fields, subscriber)
    VALUES ($1::jsonb, '{SYSTEM}')
    RETURNING unid
""", ["text"])

row = plpy.execute(plan, [json.dumps(fields)])
sub_unid = row[0]['unid']

sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'subscribe', jsonb_build_object(
        'subscriber', $2, 'target', $3, 'endpoint', $4, 'protocol', $5
    ), '00000000-0000-0000-0000-000000000001')
""", ["uuid", "text", "text", "text", "text"])
plpy.execute(sig, [sub_unid, p_subscriber_name, p_target_filter, p_endpoint, p_protocol])

plpy.notice(f'Subscription created: {p_subscriber_name} -> {p_target_filter} via {p_protocol}://{p_endpoint}')
return sub_unid
$function$
