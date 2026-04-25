-- ============================================================
-- THE ALIAS LIBRARY
--
-- Zero cognitive load. An agent thinks in networking, OS,
-- and human concepts. These aliases map every real-world
-- mental model directly to the force that does the thing.
--
-- No translation. No lookup. Think it, call it.
-- ============================================================

-- ========== NETWORKING ==========

-- Send a packet
CREATE OR REPLACE FUNCTION substrate.packet_send(p_from TEXT, p_to TEXT, p_payload TEXT, p_subject TEXT DEFAULT '')
RETURNS UUID AS $$ SELECT substrate.send(p_from, p_to, p_payload, p_subject) $$ LANGUAGE sql;

-- Broadcast a packet
CREATE OR REPLACE FUNCTION substrate.packet_broadcast(p_from TEXT, p_to TEXT[], p_payload TEXT, p_subject TEXT DEFAULT '')
RETURNS UUID[] AS $$ SELECT substrate.broadcast(p_from, p_to, p_payload, p_subject) $$ LANGUAGE sql;

-- Check the wire
CREATE OR REPLACE FUNCTION substrate.packet_capture(p_address TEXT, p_limit INT DEFAULT 50)
RETURNS JSONB AS $$ SELECT substrate.inbox(p_address, NULL, p_limit) $$ LANGUAGE sql;

-- Set QoS on a pipe
CREATE OR REPLACE FUNCTION substrate.qos(p_subscription_unid UUID, p_level INT)
RETURNS JSONB AS $$ SELECT substrate.set_governance(p_subscription_unid, p_level) $$ LANGUAGE sql;

-- Open a route
CREATE OR REPLACE FUNCTION substrate.route_add(p_subscriber TEXT, p_filter TEXT, p_endpoint TEXT, p_protocol TEXT DEFAULT 'pg', p_qos INT DEFAULT 50)
RETURNS UUID AS $$
    SELECT sub_unid FROM (
        SELECT substrate.subscribe(p_subscriber, p_filter, p_endpoint, p_protocol) as sub_unid
    ) s, LATERAL substrate.set_governance(s.sub_unid, p_qos) g
$$ LANGUAGE sql;

-- Close a route
CREATE OR REPLACE FUNCTION substrate.route_remove(p_subscription_unid UUID)
RETURNS VOID AS $$ SELECT substrate.unsubscribe(p_subscription_unid) $$ LANGUAGE sql;

-- Firewall rule
CREATE OR REPLACE FUNCTION substrate.firewall(p_blob_unid UUID, p_rule TEXT)
RETURNS BOOLEAN AS $$ SELECT substrate.gate(p_blob_unid, p_rule) $$ LANGUAGE sql;

-- MTU check
CREATE OR REPLACE FUNCTION substrate.mtu_check(p_blob_unid UUID, p_max_bytes INT)
RETURNS BOOLEAN AS $$ SELECT substrate.gate(p_blob_unid, 'size<' || p_max_bytes::text) $$ LANGUAGE sql;

-- Ping (is the pipe alive?)
CREATE OR REPLACE FUNCTION substrate.ping(p_to TEXT)
RETURNS UUID AS $$ SELECT substrate.send('SYSTEM', p_to, 'ping', 'ping', 'command') $$ LANGUAGE sql;

-- ========== OPERATING SYSTEM ==========

-- Install a program
CREATE OR REPLACE FUNCTION substrate.install_program(p_url TEXT, p_path TEXT)
RETURNS UUID AS $$ SELECT substrate.install(p_url, p_path) $$ LANGUAGE sql;

-- Run a program
CREATE OR REPLACE FUNCTION substrate.run(p_file_unid UUID, p_args TEXT[] DEFAULT '{}')
RETURNS UUID AS $$ SELECT substrate.exec(p_file_unid, p_args) $$ LANGUAGE sql;

-- Run from memory (zero disk)
CREATE OR REPLACE FUNCTION substrate.run_from_memory(p_blob_unid UUID, p_args TEXT[] DEFAULT '{}')
RETURNS TEXT AS $$ SELECT substrate.memfd_exec_compressed(p_blob_unid, p_args) $$ LANGUAGE sql;

-- Read a file into the OS
CREATE OR REPLACE FUNCTION substrate.read_file(p_path TEXT)
RETURNS UUID AS $$ SELECT substrate.ingest(p_path) $$ LANGUAGE sql;

-- Write a blob to disk
CREATE OR REPLACE FUNCTION substrate.write_file(p_blob_unid UUID)
RETURNS TEXT AS $$ SELECT substrate.materialize(p_blob_unid) $$ LANGUAGE sql;

-- Kill a blob (retire it)
CREATE OR REPLACE FUNCTION substrate.kill(p_unid UUID)
RETURNS VOID AS $$ SELECT substrate.retire(p_unid) $$ LANGUAGE sql;

-- Garbage collect
CREATE OR REPLACE FUNCTION substrate.gc(p_grace INTERVAL DEFAULT '7 days', p_mode TEXT DEFAULT 'purge')
RETURNS JSONB AS $$ SELECT substrate.reap(p_grace, p_mode, false) $$ LANGUAGE sql;

-- Disk usage
CREATE OR REPLACE FUNCTION substrate.du()
RETURNS JSONB AS $$ SELECT substrate.vacuum_report() $$ LANGUAGE sql;

-- Process list
CREATE OR REPLACE FUNCTION substrate.ps()
RETURNS TABLE(unid UUID, name TEXT, exitcode TEXT, state TEXT) AS $$
    SELECT unid,
           fields->'name'->>'value',
           fields->'exitcode'->>'value',
           fields->'state'->>'value'
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'process'
$$ LANGUAGE sql;

-- List files
CREATE OR REPLACE FUNCTION substrate.ls(p_composition TEXT DEFAULT 'file')
RETURNS TABLE(unid UUID, name TEXT, size TEXT, content_type TEXT) AS $$
    SELECT unid,
           fields->'name'->>'value',
           COALESCE(fields->'original_size'->>'value', fields->'size'->>'value', '0'),
           COALESCE(fields->'content'->>'type', '-')
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = p_composition
    AND (fields->'state' IS NULL OR fields->'state'->>'value' NOT IN ('retired', 'reaped'))
$$ LANGUAGE sql;

-- Who am I
CREATE OR REPLACE FUNCTION substrate.whoami()
RETURNS TABLE(unid UUID, name TEXT, composition TEXT) AS $$
    SELECT unid,
           fields->'name'->>'value',
           fields->'composition'->>'value'
    FROM substrate.blob
    WHERE fields->'composition'->>'value' = 'principal'
    LIMIT 1
$$ LANGUAGE sql;

-- ========== HUMAN / SOCIAL ==========

-- Say something to someone
CREATE OR REPLACE FUNCTION substrate.say(p_to TEXT, p_what TEXT)
RETURNS UUID AS $$ SELECT substrate.send(current_setting('substrate.identity', true), p_to, p_what, '', 'dm') $$ LANGUAGE sql;

-- Tell everyone something
CREATE OR REPLACE FUNCTION substrate.announce(p_what TEXT, p_to TEXT[] DEFAULT ARRAY['oa:matt','oa:joey','oa:vision','oa:jarvis','oa:kevin','oa:ari'])
RETURNS UUID[] AS $$ SELECT substrate.broadcast(current_setting('substrate.identity', true), p_to, p_what, 'announcement') $$ LANGUAGE sql;

-- Check my messages
CREATE OR REPLACE FUNCTION substrate.messages(p_status TEXT DEFAULT NULL)
RETURNS JSONB AS $$ SELECT substrate.inbox(current_setting('substrate.identity', true), p_status) $$ LANGUAGE sql;

-- Reply to a message
CREATE OR REPLACE FUNCTION substrate.respond(p_message_unid UUID, p_body TEXT)
RETURNS UUID AS $$ SELECT substrate.reply(p_message_unid, p_body) $$ LANGUAGE sql;

-- Mark done
CREATE OR REPLACE FUNCTION substrate.done(p_message_unid UUID)
RETURNS VOID AS $$ SELECT substrate.ack_message(p_message_unid, 'completed') $$ LANGUAGE sql;

-- Share a file with someone
CREATE OR REPLACE FUNCTION substrate.share(p_blob_unid UUID, p_with TEXT)
RETURNS VOID AS $$
    UPDATE substrate.blob
    SET subscriber = array_append(subscriber, p_with)
    WHERE unid = p_blob_unid
$$ LANGUAGE sql;

-- ========== GIT / SYNC ==========

-- Clone (bootstrap from events server)
CREATE OR REPLACE FUNCTION substrate.clone(p_peer TEXT DEFAULT NULL)
RETURNS INT AS $$ SELECT substrate.sync_from_manifest('SYSTEM', p_peer) $$ LANGUAGE sql;

-- Push (publish local changes)
CREATE OR REPLACE FUNCTION substrate.push(p_blob_unid UUID)
RETURNS TEXT AS $$ SELECT substrate.publish(p_blob_unid, current_setting('substrate.peer_id', true)) $$ LANGUAGE sql;

-- Pull (poll for remote changes)
CREATE OR REPLACE FUNCTION substrate.pull()
RETURNS INT AS $$ SELECT substrate.poll_events(current_setting('substrate.identity', true)) $$ LANGUAGE sql;

-- ========== STORAGE ==========

-- Zip (compress a blob)
CREATE OR REPLACE FUNCTION substrate.zip(p_blob_unid UUID)
RETURNS UUID AS $$ SELECT substrate.compress(p_blob_unid, 'zlib') $$ LANGUAGE sql;

-- Unzip (decompress)
CREATE OR REPLACE FUNCTION substrate.unzip(p_blob_unid UUID)
RETURNS BYTEA AS $$ SELECT substrate.decompress(p_blob_unid) $$ LANGUAGE sql;

-- Upload (ingest + compress)
CREATE OR REPLACE FUNCTION substrate.upload(p_path TEXT)
RETURNS UUID AS $$ SELECT substrate.ingest_compressed(p_path, 'zlib') $$ LANGUAGE sql;

-- Download (emit to file)
CREATE OR REPLACE FUNCTION substrate.download(p_blob_unid UUID, p_path TEXT)
RETURNS TEXT AS $$ SELECT substrate.emit(p_blob_unid, p_path, 'file', false) $$ LANGUAGE sql;

-- Ship (compress + emit to endpoint)
CREATE OR REPLACE FUNCTION substrate.ship(p_blob_unid UUID, p_endpoint TEXT, p_protocol TEXT DEFAULT 'http')
RETURNS TEXT AS $$ SELECT substrate.emit(p_blob_unid, p_endpoint, p_protocol, true) $$ LANGUAGE sql;

-- ========== PIPES ==========

-- Open a pipe
CREATE OR REPLACE FUNCTION substrate.pipe_open(p_name TEXT, p_target TEXT, p_endpoint TEXT, p_qos INT DEFAULT 50)
RETURNS UUID AS $$ SELECT substrate.route_add(p_name, p_target, p_endpoint, 'pg', p_qos) $$ LANGUAGE sql;

-- Close a pipe
CREATE OR REPLACE FUNCTION substrate.pipe_close(p_subscription_unid UUID)
RETURNS VOID AS $$ SELECT substrate.unsubscribe(p_subscription_unid) $$ LANGUAGE sql;

-- Flush a pipe (drain queued emissions)
CREATE OR REPLACE FUNCTION substrate.pipe_flush(p_subscription_unid UUID)
RETURNS INT AS $$ SELECT substrate.drain(p_subscription_unid) $$ LANGUAGE sql;

-- Pipe status
CREATE OR REPLACE FUNCTION substrate.pipe_status()
RETURNS TABLE(subscriber TEXT, target TEXT, endpoint TEXT, governance INT, pipe_status TEXT, queue_depth INT) AS $$
    SELECT
        s.fields->'subscriber'->>'value',
        s.fields->'target'->>'value',
        s.fields->'endpoint'->>'value',
        COALESCE((s.fields->'governance'->>'value')::int, 50),
        COALESCE(p.fields->'status'->>'value', 'unknown'),
        COALESCE((p.fields->'queue_depth'->>'value')::int, 0)
    FROM substrate.blob s
    LEFT JOIN substrate.blob p ON p.fields->'composition'->>'value' = 'pipe_state'
        AND p.fields->'subscription'->>'value' = s.unid::text
    WHERE s.fields->'composition'->>'value' = 'subscription'
    AND (s.fields->'state' IS NULL OR s.fields->'state'->>'value' != 'retired')
$$ LANGUAGE sql;

-- ========== CENSUS ==========

-- How big is the universe
CREATE OR REPLACE FUNCTION substrate.census()
RETURNS TABLE(metric TEXT, value BIGINT) AS $$
    SELECT 'blobs', count(*) FROM substrate.blob
    UNION ALL SELECT 'signals', count(*) FROM substrate.signal
    UNION ALL SELECT 'files', count(*) FROM substrate.blob WHERE fields->'composition'->>'value' = 'file'
    UNION ALL SELECT 'messages', count(*) FROM substrate.blob WHERE fields->'composition'->>'value' = 'message'
    UNION ALL SELECT 'subscriptions', count(*) FROM substrate.blob WHERE fields->'composition'->>'value' = 'subscription'
    UNION ALL SELECT 'retired', count(*) FROM substrate.blob WHERE retired_at IS NOT NULL
$$ LANGUAGE sql;
