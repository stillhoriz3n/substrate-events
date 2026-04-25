-- ============================================================
-- SUBSTRATE LIBRARY: lib.protocol
-- HTTP, DNS, TCP/UDP, RTP, MIME, wire formats, streaming
-- ============================================================

-- ===== HTTP =====

-- HTTP status code to reason phrase + category
CREATE OR REPLACE FUNCTION substrate.http_status(code INT)
RETURNS JSONB AS $$
import json
statuses = {
    100:'Continue',101:'Switching Protocols',102:'Processing',103:'Early Hints',
    200:'OK',201:'Created',202:'Accepted',203:'Non-Authoritative Information',
    204:'No Content',205:'Reset Content',206:'Partial Content',207:'Multi-Status',
    301:'Moved Permanently',302:'Found',303:'See Other',304:'Not Modified',
    307:'Temporary Redirect',308:'Permanent Redirect',
    400:'Bad Request',401:'Unauthorized',402:'Payment Required',403:'Forbidden',
    404:'Not Found',405:'Method Not Allowed',406:'Not Acceptable',408:'Request Timeout',
    409:'Conflict',410:'Gone',411:'Length Required',412:'Precondition Failed',
    413:'Content Too Large',414:'URI Too Long',415:'Unsupported Media Type',
    416:'Range Not Satisfiable',418:"I'm a Teapot",421:'Misdirected Request',
    422:'Unprocessable Content',423:'Locked',425:'Too Early',426:'Upgrade Required',
    428:'Precondition Required',429:'Too Many Requests',431:'Request Header Fields Too Large',
    451:'Unavailable For Legal Reasons',
    500:'Internal Server Error',501:'Not Implemented',502:'Bad Gateway',
    503:'Service Unavailable',504:'Gateway Timeout',505:'HTTP Version Not Supported',
    507:'Insufficient Storage',508:'Loop Detected',511:'Network Authentication Required',
}
cat_map = {1:'informational',2:'success',3:'redirection',4:'client_error',5:'server_error'}
reason = statuses.get(code, 'Unknown')
cat = cat_map.get(code // 100, 'unknown')
return json.dumps({'code':code,'reason':reason,'category':cat})
$$ LANGUAGE plpython3u IMMUTABLE;

-- HTTP method properties
CREATE OR REPLACE FUNCTION substrate.http_method_info(method TEXT)
RETURNS JSONB AS $$
import json
methods = {
    'GET':    {'safe':True,'idempotent':True,'cacheable':True,'body':False},
    'HEAD':   {'safe':True,'idempotent':True,'cacheable':True,'body':False},
    'POST':   {'safe':False,'idempotent':False,'cacheable':False,'body':True},
    'PUT':    {'safe':False,'idempotent':True,'cacheable':False,'body':True},
    'DELETE': {'safe':False,'idempotent':True,'cacheable':False,'body':False},
    'PATCH':  {'safe':False,'idempotent':False,'cacheable':False,'body':True},
    'OPTIONS':{'safe':True,'idempotent':True,'cacheable':False,'body':False},
    'CONNECT':{'safe':False,'idempotent':False,'cacheable':False,'body':False},
    'TRACE':  {'safe':True,'idempotent':True,'cacheable':False,'body':False},
}
m = methods.get(method.upper())
return json.dumps(m) if m else json.dumps({'error':'unknown method'})
$$ LANGUAGE plpython3u IMMUTABLE;

-- HTTP Cache-Control max-age to human duration
CREATE OR REPLACE FUNCTION substrate.cache_ttl(max_age_sec INT)
RETURNS TEXT AS $$ SELECT substrate.human_duration(max_age_sec::float8) $$ LANGUAGE sql IMMUTABLE;

-- ===== MIME TYPES =====

-- Extension to MIME type
CREATE OR REPLACE FUNCTION substrate.ext_to_mime(ext TEXT)
RETURNS TEXT AS $$
m = {
    # Text
    'html':'text/html','htm':'text/html','css':'text/css','js':'application/javascript',
    'json':'application/json','xml':'application/xml','csv':'text/csv','txt':'text/plain',
    'md':'text/markdown','yaml':'text/yaml','yml':'text/yaml','toml':'application/toml',
    'svg':'image/svg+xml','wasm':'application/wasm',
    # Images
    'png':'image/png','jpg':'image/jpeg','jpeg':'image/jpeg','gif':'image/gif',
    'webp':'image/webp','avif':'image/avif','ico':'image/x-icon','bmp':'image/bmp',
    'tiff':'image/tiff','tif':'image/tiff','heic':'image/heic','jxl':'image/jxl',
    'psd':'image/vnd.adobe.photoshop','exr':'image/x-exr',
    # Audio
    'mp3':'audio/mpeg','wav':'audio/wav','flac':'audio/flac','ogg':'audio/ogg',
    'opus':'audio/opus','aac':'audio/aac','m4a':'audio/mp4','wma':'audio/x-ms-wma',
    'aiff':'audio/aiff','aif':'audio/aiff','mid':'audio/midi','midi':'audio/midi',
    'ac3':'audio/ac3','dts':'audio/vnd.dts',
    # Video
    'mp4':'video/mp4','webm':'video/webm','mkv':'video/x-matroska','avi':'video/x-msvideo',
    'mov':'video/quicktime','wmv':'video/x-ms-wmv','flv':'video/x-flv',
    'ts':'video/mp2t','m3u8':'application/vnd.apple.mpegurl','mpd':'application/dash+xml',
    'mxf':'application/mxf','ogv':'video/ogg',
    # Archives
    'zip':'application/zip','gz':'application/gzip','tar':'application/x-tar',
    'bz2':'application/x-bzip2','xz':'application/x-xz','7z':'application/x-7z-compressed',
    'rar':'application/vnd.rar','zst':'application/zstd',
    # Documents
    'pdf':'application/pdf','doc':'application/msword',
    'docx':'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'xls':'application/vnd.ms-excel',
    'xlsx':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'ppt':'application/vnd.ms-powerpoint',
    'pptx':'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    # Fonts
    'woff':'font/woff','woff2':'font/woff2','ttf':'font/ttf','otf':'font/otf','eot':'application/vnd.ms-fontobject',
    # Other
    'exe':'application/x-executable','dll':'application/x-msdownload',
    'apk':'application/vnd.android.package-archive','dmg':'application/x-apple-diskimage',
    'iso':'application/x-iso9660-image','sql':'application/sql',
    'graphql':'application/graphql','protobuf':'application/protobuf',
}
e = ext.lower().lstrip('.')
return m.get(e, 'application/octet-stream')
$$ LANGUAGE plpython3u IMMUTABLE;

-- MIME type to category
CREATE OR REPLACE FUNCTION substrate.mime_category(mime TEXT)
RETURNS TEXT AS $$
t = mime.split('/')[0] if '/' in mime else mime
cats = {'text':'document','image':'image','audio':'audio','video':'video','font':'font','application':'application','model':'3d-model'}
return cats.get(t, 'other')
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== TCP/UDP =====

-- TCP overhead per packet (bytes): IP header + TCP header
CREATE OR REPLACE FUNCTION substrate.tcp_overhead(ipv6 BOOLEAN DEFAULT FALSE)
RETURNS INT AS $$ SELECT CASE WHEN ipv6 THEN 60 ELSE 40 END $$ LANGUAGE sql IMMUTABLE;

-- TCP MSS (Maximum Segment Size) from MTU
CREATE OR REPLACE FUNCTION substrate.tcp_mss(mtu INT DEFAULT 1500, ipv6 BOOLEAN DEFAULT FALSE)
RETURNS INT AS $$ SELECT mtu - CASE WHEN ipv6 THEN 60 ELSE 40 END $$ LANGUAGE sql IMMUTABLE;

-- TCP goodput (payload efficiency): MSS / (MSS + overhead)
CREATE OR REPLACE FUNCTION substrate.tcp_efficiency(mtu INT DEFAULT 1500, ipv6 BOOLEAN DEFAULT FALSE)
RETURNS FLOAT8 AS $$
overhead = 60 if ipv6 else 40
mss = mtu - overhead
return mss / float(mtu)
$$ LANGUAGE plpython3u IMMUTABLE;

-- TCP window size to bandwidth (bytes/sec): window_bytes / rtt_sec
CREATE OR REPLACE FUNCTION substrate.tcp_max_throughput(window_bytes INT, rtt_ms FLOAT8)
RETURNS FLOAT8 AS $$ SELECT window_bytes::float8 / (rtt_ms / 1000.0) $$ LANGUAGE sql IMMUTABLE;

-- TCP congestion window estimate (AIMD): cwnd after n RTTs from slow start
CREATE OR REPLACE FUNCTION substrate.tcp_cwnd_slowstart(initial_cwnd INT, n_rtts INT)
RETURNS INT AS $$ SELECT initial_cwnd * power(2, n_rtts)::int $$ LANGUAGE sql IMMUTABLE;

-- Well-known port lookup
CREATE OR REPLACE FUNCTION substrate.port_info(port_num INT)
RETURNS JSONB AS $$
import json
ports = {
    20:'FTP Data',21:'FTP Control',22:'SSH',23:'Telnet',25:'SMTP',
    53:'DNS',67:'DHCP Server',68:'DHCP Client',69:'TFTP',
    80:'HTTP',110:'POP3',119:'NNTP',123:'NTP',143:'IMAP',
    161:'SNMP',162:'SNMP Trap',179:'BGP',194:'IRC',
    443:'HTTPS',445:'SMB',465:'SMTPS',514:'Syslog',
    587:'SMTP Submission',636:'LDAPS',993:'IMAPS',995:'POP3S',
    1080:'SOCKS',1433:'MSSQL',1521:'Oracle',1883:'MQTT',
    2049:'NFS',3306:'MySQL',3389:'RDP',5432:'PostgreSQL',
    5672:'AMQP',5900:'VNC',6379:'Redis',6443:'Kubernetes API',
    8080:'HTTP Alt',8443:'HTTPS Alt',8883:'MQTT TLS',
    9090:'Prometheus',9200:'Elasticsearch',9418:'Git',
    11211:'Memcached',27017:'MongoDB',
    # Streaming
    554:'RTSP',1935:'RTMP',5004:'RTP',5005:'RTCP',
    8554:'RTSP Alt',
    # MythOS specific
    5985:'WinRM HTTP',5986:'WinRM HTTPS',
    6969:'BabelClient',6970:'Inject',8080:'BabelServer',
}
name = ports.get(port_num)
if name:
    cat = 'well-known' if port_num < 1024 else 'registered' if port_num < 49152 else 'dynamic'
    return json.dumps({'port':port_num,'service':name,'range':cat})
cat = 'well-known' if port_num < 1024 else 'registered' if port_num < 49152 else 'dynamic'
return json.dumps({'port':port_num,'service':'unknown','range':cat})
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== DNS =====

-- DNS record type lookup
CREATE OR REPLACE FUNCTION substrate.dns_record_type(rtype TEXT)
RETURNS JSONB AS $$
import json
types = {
    'A':     {'value':1,'desc':'IPv4 address','rfc':'RFC 1035'},
    'AAAA':  {'value':28,'desc':'IPv6 address','rfc':'RFC 3596'},
    'CNAME': {'value':5,'desc':'Canonical name (alias)','rfc':'RFC 1035'},
    'MX':    {'value':15,'desc':'Mail exchange','rfc':'RFC 1035'},
    'NS':    {'value':2,'desc':'Name server','rfc':'RFC 1035'},
    'PTR':   {'value':12,'desc':'Pointer (reverse DNS)','rfc':'RFC 1035'},
    'SOA':   {'value':6,'desc':'Start of authority','rfc':'RFC 1035'},
    'SRV':   {'value':33,'desc':'Service locator','rfc':'RFC 2782'},
    'TXT':   {'value':16,'desc':'Text record','rfc':'RFC 1035'},
    'CAA':   {'value':257,'desc':'Cert authority authorization','rfc':'RFC 8659'},
    'DNSKEY':{'value':48,'desc':'DNSSEC public key','rfc':'RFC 4034'},
    'DS':    {'value':43,'desc':'Delegation signer','rfc':'RFC 4034'},
    'HTTPS': {'value':65,'desc':'HTTPS service binding','rfc':'RFC 9460'},
    'SVCB':  {'value':64,'desc':'Service binding','rfc':'RFC 9460'},
    'NAPTR': {'value':35,'desc':'Naming authority pointer','rfc':'RFC 3403'},
    'TLSA':  {'value':52,'desc':'TLS cert association (DANE)','rfc':'RFC 6698'},
}
t = types.get(rtype.upper())
return json.dumps(t) if t else json.dumps({'error':'unknown','known':list(types.keys())})
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== RTP / STREAMING =====

-- RTP timestamp to seconds (given clock rate)
CREATE OR REPLACE FUNCTION substrate.rtp_ts_to_sec(rtp_timestamp BIGINT, clock_rate INT DEFAULT 90000)
RETURNS FLOAT8 AS $$ SELECT rtp_timestamp::float8 / clock_rate $$ LANGUAGE sql IMMUTABLE;

-- Seconds to RTP timestamp
CREATE OR REPLACE FUNCTION substrate.sec_to_rtp_ts(seconds FLOAT8, clock_rate INT DEFAULT 90000)
RETURNS BIGINT AS $$ SELECT (seconds * clock_rate)::bigint $$ LANGUAGE sql IMMUTABLE;

-- RTP packet size (bytes): header + payload
-- Header: 12 bytes fixed + 4*csrc_count + extensions
CREATE OR REPLACE FUNCTION substrate.rtp_packet_size(payload_bytes INT, csrc_count INT DEFAULT 0, extension_bytes INT DEFAULT 0)
RETURNS INT AS $$ SELECT 12 + 4 * csrc_count + extension_bytes + payload_bytes $$ LANGUAGE sql IMMUTABLE;

-- RTP jitter buffer size (ms) recommendation
CREATE OR REPLACE FUNCTION substrate.jitter_buffer_ms(avg_jitter_ms FLOAT8, safety_factor FLOAT8 DEFAULT 2.0)
RETURNS FLOAT8 AS $$ SELECT avg_jitter_ms * safety_factor $$ LANGUAGE sql IMMUTABLE;

-- HLS segment count for duration at segment length
CREATE OR REPLACE FUNCTION substrate.hls_segments(duration_sec FLOAT8, segment_sec FLOAT8 DEFAULT 6.0)
RETURNS INT AS $$ SELECT ceil(duration_sec / segment_sec)::int $$ LANGUAGE sql IMMUTABLE;

-- ABR (Adaptive Bitrate) ladder generator
CREATE OR REPLACE FUNCTION substrate.abr_ladder(max_width INT, max_height INT, max_bitrate_mbps FLOAT8)
RETURNS JSONB AS $$
import json
# Standard rungs, filtered to what fits
rungs = [
    (3840,2160,0.70), (2560,1440,0.45), (1920,1080,0.30),
    (1280,720,0.15), (960,540,0.08), (640,360,0.04), (426,240,0.02)
]
ladder = []
for w,h,ratio in rungs:
    if w <= max_width and h <= max_height:
        br = round(max_bitrate_mbps * ratio / rungs[0][2], 2)
        br = min(br, max_bitrate_mbps)
        ladder.append({'width':w,'height':h,'bitrate_mbps':br})
return json.dumps(ladder)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== STREAMING PROTOCOLS =====

-- Streaming protocol info
CREATE OR REPLACE FUNCTION substrate.streaming_protocol_info(proto TEXT)
RETURNS JSONB AS $$
import json
protocols = {
    'hls':      {'name':'HTTP Live Streaming','port':443,'transport':'HTTP/HTTPS','latency':'6-30s',
                 'adaptive':True,'drm':['FairPlay','Widevine'],'apple':True,'spec':'RFC 8216'},
    'dash':     {'name':'MPEG-DASH','port':443,'transport':'HTTP/HTTPS','latency':'6-30s',
                 'adaptive':True,'drm':['Widevine','PlayReady'],'spec':'ISO 23009'},
    'rtmp':     {'name':'RTMP','port':1935,'transport':'TCP','latency':'1-3s',
                 'adaptive':False,'note':'ingest only (deprecated for playback)'},
    'rtsp':     {'name':'RTSP','port':554,'transport':'TCP+UDP(RTP)','latency':'<1s',
                 'adaptive':False,'spec':'RFC 7826'},
    'rtp':      {'name':'RTP','port':5004,'transport':'UDP','latency':'<100ms',
                 'adaptive':False,'spec':'RFC 3550'},
    'srt':      {'name':'SRT','port':9000,'transport':'UDP','latency':'120ms-2s',
                 'adaptive':False,'encryption':'AES-128/256','open_source':True},
    'webrtc':   {'name':'WebRTC','port':'443/UDP','transport':'DTLS-SRTP/ICE','latency':'<500ms',
                 'adaptive':True,'p2p':True,'spec':'RFC 8825'},
    'whip':     {'name':'WHIP','port':443,'transport':'HTTP+WebRTC','latency':'<500ms',
                 'adaptive':True,'direction':'ingest','spec':'RFC 9725 (draft)'},
    'whep':     {'name':'WHEP','port':443,'transport':'HTTP+WebRTC','latency':'<500ms',
                 'adaptive':True,'direction':'egress','spec':'draft'},
    'll-hls':   {'name':'Low-Latency HLS','port':443,'transport':'HTTP/HTTPS','latency':'2-4s',
                 'adaptive':True,'apple':True},
    'll-dash':  {'name':'Low-Latency DASH','port':443,'transport':'HTTP/HTTPS','latency':'2-4s',
                 'adaptive':True},
    'ndi':      {'name':'NDI','port':'5353/mDNS','transport':'TCP/UDP','latency':'<1 frame',
                 'adaptive':False,'note':'LAN video transport (NewTek)','mcast':True},
    'rist':     {'name':'RIST','port':'variable','transport':'UDP+ARQ','latency':'<1s',
                 'adaptive':False,'spec':'VSF TR-06'},
    'zixi':     {'name':'Zixi','port':'various','transport':'UDP','latency':'<1s',
                 'adaptive':False,'note':'Commercial contribution codec'},
    'mpeg-ts':  {'name':'MPEG Transport Stream','port':'various','transport':'UDP/TCP','latency':'1-5s',
                 'adaptive':False,'broadcast':True,'spec':'ISO 13818-1'},
}
p = protocols.get(proto.lower().replace(' ','-').replace('_','-'))
return json.dumps(p) if p else json.dumps({'error':'unknown','known':list(protocols.keys())})
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== WIRE FORMAT SIZING =====

-- Ethernet frame size (bytes): preamble + header + payload + FCS
CREATE OR REPLACE FUNCTION substrate.ethernet_frame_size(payload_bytes INT, vlan_tagged BOOLEAN DEFAULT FALSE)
RETURNS INT AS $$
SELECT 8 + 14 + CASE WHEN vlan_tagged THEN 4 ELSE 0 END + payload_bytes + 4
$$ LANGUAGE sql IMMUTABLE;

-- IP packet overhead
CREATE OR REPLACE FUNCTION substrate.ip_overhead(ipv6 BOOLEAN DEFAULT FALSE)
RETURNS INT AS $$ SELECT CASE WHEN ipv6 THEN 40 ELSE 20 END $$ LANGUAGE sql IMMUTABLE;

-- UDP datagram size: header(8) + payload
CREATE OR REPLACE FUNCTION substrate.udp_packet_size(payload_bytes INT)
RETURNS INT AS $$ SELECT 8 + payload_bytes $$ LANGUAGE sql IMMUTABLE;

-- Max UDP payload in single packet (no fragmentation)
CREATE OR REPLACE FUNCTION substrate.max_udp_payload(mtu INT DEFAULT 1500, ipv6 BOOLEAN DEFAULT FALSE)
RETURNS INT AS $$ SELECT mtu - CASE WHEN ipv6 THEN 40 ELSE 20 END - 8 $$ LANGUAGE sql IMMUTABLE;

-- WebSocket frame overhead (bytes)
CREATE OR REPLACE FUNCTION substrate.ws_frame_overhead(payload_bytes INT, masked BOOLEAN DEFAULT TRUE)
RETURNS INT AS $$
base = 2
if payload_bytes > 65535:
    base += 8
elif payload_bytes > 125:
    base += 2
if masked:
    base += 4
return base
$$ LANGUAGE plpython3u IMMUTABLE;

-- gRPC frame size: 5-byte prefix + message
CREATE OR REPLACE FUNCTION substrate.grpc_frame_size(message_bytes INT)
RETURNS INT AS $$ SELECT 5 + message_bytes $$ LANGUAGE sql IMMUTABLE;

-- QUIC packet overhead estimate (bytes)
CREATE OR REPLACE FUNCTION substrate.quic_overhead(long_header BOOLEAN DEFAULT FALSE)
RETURNS INT AS $$ SELECT CASE WHEN long_header THEN 36 ELSE 20 END $$ LANGUAGE sql IMMUTABLE;

-- TLS record overhead (bytes): header(5) + MAC(varies) + padding
CREATE OR REPLACE FUNCTION substrate.tls_overhead(tls_version TEXT DEFAULT '1.3')
RETURNS INT AS $$
if tls_version == '1.3': return 22  # 5 header + 16 AEAD tag + 1 content type
if tls_version == '1.2': return 37  # 5 header + 16 IV + 16 MAC
return 40  # conservative estimate
$$ LANGUAGE plpython3u IMMUTABLE;
