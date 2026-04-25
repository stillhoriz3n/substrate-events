CREATE OR REPLACE FUNCTION substrate.unsubscribe(p_subscription_unid uuid)
 RETURNS void
 LANGUAGE plpython3u
AS $function$

plan = plpy.prepare("""
    UPDATE substrate.blob 
    SET fields = jsonb_set(fields, '{state}', jsonb_build_object('type', 'utf8', 'value', 'retired'))
    WHERE unid = $1 AND fields->'composition'->>'value' = 'subscription'
""", ["uuid"])
plpy.execute(plan, [p_subscription_unid])

sig = plpy.prepare("""
    INSERT INTO substrate.signal (blob_unid, signal_type, detail, actor)
    VALUES ($1, 'unsubscribe', '{}', '00000000-0000-0000-0000-000000000001')
""", ["uuid"])
plpy.execute(sig, [p_subscription_unid])
$function$
