CREATE OR REPLACE FUNCTION substrate.share(p_blob_unid uuid, p_with text)
 RETURNS void
 LANGUAGE sql
AS $function$
    UPDATE substrate.blob
    SET subscriber = array_append(subscriber, p_with)
    WHERE unid = p_blob_unid
$function$
