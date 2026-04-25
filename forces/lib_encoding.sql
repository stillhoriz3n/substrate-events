-- ============================================================
-- SUBSTRATE LIBRARY: lib.encoding
-- Codec math, compression, character sets, serialization
-- ============================================================

-- ===== COMPRESSION =====

-- Compression ratio
CREATE OR REPLACE FUNCTION substrate.compression_ratio(original_bytes BIGINT, compressed_bytes BIGINT)
RETURNS FLOAT8 AS $$ SELECT original_bytes::float8 / NULLIF(compressed_bytes, 0) $$ LANGUAGE sql IMMUTABLE;

-- Space savings percentage
CREATE OR REPLACE FUNCTION substrate.space_savings_pct(original_bytes BIGINT, compressed_bytes BIGINT)
RETURNS FLOAT8 AS $$ SELECT 100.0 * (1 - compressed_bytes::float8 / NULLIF(original_bytes, 0)) $$ LANGUAGE sql IMMUTABLE;

-- Bits per pixel (image/video compression metric)
CREATE OR REPLACE FUNCTION substrate.bpp(file_bytes BIGINT, width INT, height INT, n_frames INT DEFAULT 1)
RETURNS FLOAT8 AS $$ SELECT (file_bytes * 8.0) / (width::float8 * height * n_frames) $$ LANGUAGE sql IMMUTABLE;

-- Compression algorithm info
CREATE OR REPLACE FUNCTION substrate.compression_info(algo TEXT)
RETURNS JSONB AS $$
import json
algos = {
    'gzip':    {'type':'lossless','family':'DEFLATE','ratio':'2-5x','speed':'medium','rfc':'RFC 1952'},
    'zlib':    {'type':'lossless','family':'DEFLATE','ratio':'2-5x','speed':'medium','rfc':'RFC 1950'},
    'deflate': {'type':'lossless','family':'DEFLATE','ratio':'2-5x','speed':'medium','rfc':'RFC 1951'},
    'brotli':  {'type':'lossless','family':'LZ77+Huffman','ratio':'3-8x','speed':'slow compress/fast decompress','rfc':'RFC 7932'},
    'zstd':    {'type':'lossless','family':'LZ77+FSE','ratio':'2-6x','speed':'very fast','rfc':'RFC 8878'},
    'lz4':     {'type':'lossless','family':'LZ77','ratio':'2-3x','speed':'fastest','rfc':'N/A'},
    'snappy':  {'type':'lossless','family':'LZ77','ratio':'1.5-2x','speed':'fastest','rfc':'N/A'},
    'bzip2':   {'type':'lossless','family':'BWT+Huffman','ratio':'4-8x','speed':'slow','rfc':'N/A'},
    'xz':      {'type':'lossless','family':'LZMA2','ratio':'5-10x','speed':'very slow','rfc':'N/A'},
    'lzma':    {'type':'lossless','family':'LZMA','ratio':'5-10x','speed':'very slow','rfc':'N/A'},
    'jpeg':    {'type':'lossy','family':'DCT','ratio':'10-30x','speed':'fast','note':'images only'},
    'jpeg2000':{'type':'lossy/lossless','family':'DWT','ratio':'15-50x','speed':'slow','note':'images, DCI cinema'},
    'webp':    {'type':'lossy/lossless','family':'VP8-based','ratio':'10-40x','speed':'medium','note':'images'},
    'avif':    {'type':'lossy/lossless','family':'AV1','ratio':'15-50x','speed':'slow','note':'images'},
    'jxl':     {'type':'lossy/lossless','family':'VarDCT+Modular','ratio':'15-60x','speed':'medium','note':'JPEG XL'},
    'png':     {'type':'lossless','family':'DEFLATE','ratio':'2-4x','speed':'medium','note':'images'},
}
a = algos.get(algo.lower().replace('-',''))
return json.dumps(a) if a else json.dumps({'error':'unknown','known':list(algos.keys())})
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== BITRATE MATH =====

-- Bits to human-readable bitrate string
CREATE OR REPLACE FUNCTION substrate.human_bitrate(bps FLOAT8)
RETURNS TEXT AS $$
for unit in ['bps','Kbps','Mbps','Gbps','Tbps']:
    if abs(bps) < 1000 or unit == 'Tbps':
        return f'{bps:.1f} {unit}'
    bps /= 1000
return f'{bps:.1f} Tbps'
$$ LANGUAGE plpython3u IMMUTABLE;

-- Parse bitrate string to bits per second
CREATE OR REPLACE FUNCTION substrate.parse_bitrate(input TEXT)
RETURNS FLOAT8 AS $$
import re
m = re.match(r'([\d.]+)\s*(bps|kbps|mbps|gbps|tbps)', input.strip(), re.I)
if not m: return None
val = float(m.group(1))
unit = m.group(2).lower()
mult = {'bps':1,'kbps':1e3,'mbps':1e6,'gbps':1e9,'tbps':1e12}
return val * mult.get(unit, 1)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Quality metric: PSNR (Peak Signal-to-Noise Ratio)
CREATE OR REPLACE FUNCTION substrate.psnr(mse FLOAT8, max_val FLOAT8 DEFAULT 255.0)
RETURNS FLOAT8 AS $$
import math
if mse <= 0: return float('inf')
return 10 * math.log10(max_val**2 / mse)
$$ LANGUAGE plpython3u IMMUTABLE;

-- MSE from PSNR
CREATE OR REPLACE FUNCTION substrate.psnr_to_mse(psnr_db FLOAT8, max_val FLOAT8 DEFAULT 255.0)
RETURNS FLOAT8 AS $$ SELECT power(max_val, 2) / power(10, psnr_db / 10.0) $$ LANGUAGE sql IMMUTABLE;

-- SSIM quality descriptor from value
CREATE OR REPLACE FUNCTION substrate.ssim_quality(ssim FLOAT8)
RETURNS TEXT AS $$
if ssim >= 0.99: return 'imperceptible'
if ssim >= 0.95: return 'excellent'
if ssim >= 0.90: return 'good'
if ssim >= 0.80: return 'fair'
if ssim >= 0.60: return 'poor'
return 'bad'
$$ LANGUAGE plpython3u IMMUTABLE;

-- VMAF quality descriptor from value
CREATE OR REPLACE FUNCTION substrate.vmaf_quality(vmaf FLOAT8)
RETURNS TEXT AS $$
if vmaf >= 95: return 'excellent'
if vmaf >= 80: return 'good'
if vmaf >= 60: return 'fair'
if vmaf >= 40: return 'poor'
return 'bad'
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== CHARACTER ENCODING =====

-- Detect likely encoding of a byte sequence
CREATE OR REPLACE FUNCTION substrate.guess_encoding(raw_bytes BYTEA)
RETURNS TEXT AS $$
b = bytes(raw_bytes)
# BOM detection
if b[:3] == b'\xef\xbb\xbf': return 'UTF-8 (BOM)'
if b[:2] in (b'\xff\xfe', b'\xfe\xff'): return 'UTF-16'
if b[:4] in (b'\xff\xfe\x00\x00', b'\x00\x00\xfe\xff'): return 'UTF-32'
# UTF-8 validation
try:
    b.decode('utf-8')
    return 'UTF-8'
except: pass
try:
    b.decode('ascii')
    return 'ASCII'
except: pass
# Heuristic: check for high bytes
high = sum(1 for x in b if x > 127)
if high == 0: return 'ASCII'
if high / len(b) > 0.3: return 'likely binary'
return 'likely Latin-1/Windows-1252'
$$ LANGUAGE plpython3u IMMUTABLE;

-- UTF-8 byte length of a string
CREATE OR REPLACE FUNCTION substrate.utf8_len(input TEXT)
RETURNS INT AS $$ SELECT octet_length(input) $$ LANGUAGE sql IMMUTABLE;

-- Count Unicode codepoints
CREATE OR REPLACE FUNCTION substrate.codepoint_count(input TEXT)
RETURNS INT AS $$ SELECT char_length(input) $$ LANGUAGE sql IMMUTABLE;

-- String to codepoints array
CREATE OR REPLACE FUNCTION substrate.to_codepoints(input TEXT)
RETURNS INT[] AS $$
return [ord(c) for c in input]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Codepoints to string
CREATE OR REPLACE FUNCTION substrate.from_codepoints(cps INT[])
RETURNS TEXT AS $$
return ''.join(chr(c) for c in cps)
$$ LANGUAGE plpython3u IMMUTABLE;

-- URL encode
CREATE OR REPLACE FUNCTION substrate.url_encode(input TEXT)
RETURNS TEXT AS $$
from urllib.parse import quote
return quote(input)
$$ LANGUAGE plpython3u IMMUTABLE;

-- URL decode
CREATE OR REPLACE FUNCTION substrate.url_decode(input TEXT)
RETURNS TEXT AS $$
from urllib.parse import unquote
return unquote(input)
$$ LANGUAGE plpython3u IMMUTABLE;

-- HTML entity encode
CREATE OR REPLACE FUNCTION substrate.html_encode(input TEXT)
RETURNS TEXT AS $$
from html import escape
return escape(input)
$$ LANGUAGE plpython3u IMMUTABLE;

-- HTML entity decode
CREATE OR REPLACE FUNCTION substrate.html_decode(input TEXT)
RETURNS TEXT AS $$
from html import unescape
return unescape(input)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== SERIALIZATION SIZING =====

-- JSON serialized size estimate (bytes)
CREATE OR REPLACE FUNCTION substrate.json_size(doc JSONB)
RETURNS INT AS $$ SELECT octet_length(doc::text) $$ LANGUAGE sql IMMUTABLE;

-- Protobuf varint size for an integer
CREATE OR REPLACE FUNCTION substrate.varint_size(val BIGINT)
RETURNS INT AS $$
if val < 0: return 10  # protobuf negative = 10 bytes
n = 1
v = val
while v >= 128:
    v >>= 7
    n += 1
return n
$$ LANGUAGE plpython3u IMMUTABLE;

-- Base64 encoded size from raw bytes
CREATE OR REPLACE FUNCTION substrate.base64_size(raw_bytes INT)
RETURNS INT AS $$ SELECT ((raw_bytes + 2) / 3) * 4 $$ LANGUAGE sql IMMUTABLE;

-- Raw bytes from base64 encoded size
CREATE OR REPLACE FUNCTION substrate.base64_decoded_size(encoded_len INT)
RETURNS INT AS $$ SELECT (encoded_len / 4) * 3 $$ LANGUAGE sql IMMUTABLE;

-- Container format info
CREATE OR REPLACE FUNCTION substrate.container_info(fmt TEXT)
RETURNS JSONB AS $$
import json
containers = {
    'mp4':  {'name':'MPEG-4 Part 14','ext':'.mp4','video':['H.264','H.265','AV1'],'audio':['AAC','AC3','Opus'],'streaming':True,'chapters':True},
    'mkv':  {'name':'Matroska','ext':'.mkv','video':['any'],'audio':['any'],'streaming':False,'chapters':True,'subtitles':True},
    'webm': {'name':'WebM','ext':'.webm','video':['VP8','VP9','AV1'],'audio':['Vorbis','Opus'],'streaming':True,'web_native':True},
    'mov':  {'name':'QuickTime','ext':'.mov','video':['H.264','H.265','ProRes'],'audio':['AAC','PCM','ALAC'],'streaming':False},
    'avi':  {'name':'AVI','ext':'.avi','video':['any (legacy)'],'audio':['PCM','MP3'],'streaming':False,'legacy':True},
    'ts':   {'name':'MPEG Transport Stream','ext':'.ts','video':['H.264','H.265','MPEG-2'],'audio':['AAC','AC3','MP2'],'streaming':True,'broadcast':True},
    'mxf':  {'name':'Material Exchange Format','ext':'.mxf','video':['DNxHR','ProRes','XDCAM'],'audio':['PCM'],'broadcast':True,'professional':True},
    'flv':  {'name':'Flash Video','ext':'.flv','video':['H.264','VP6'],'audio':['AAC','MP3'],'streaming':True,'legacy':True},
    'ogg':  {'name':'OGG','ext':'.ogg','video':['Theora'],'audio':['Vorbis','Opus','FLAC'],'streaming':False,'open':True},
    'hls':  {'name':'HTTP Live Streaming','ext':'.m3u8','video':['H.264','H.265'],'audio':['AAC','AC3'],'streaming':True,'adaptive':True,'apple':True},
    'dash': {'name':'MPEG-DASH','ext':'.mpd','video':['any'],'audio':['any'],'streaming':True,'adaptive':True,'standard':'ISO 23009'},
}
c = containers.get(fmt.lower().replace('.',''))
return json.dumps(c) if c else json.dumps({'error':'unknown','known':list(containers.keys())})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Image format info
CREATE OR REPLACE FUNCTION substrate.image_format_info(fmt TEXT)
RETURNS JSONB AS $$
import json
formats = {
    'jpeg':  {'type':'lossy','channels':'YCbCr','depth':'8','alpha':False,'animation':False,'web':True,'mime':'image/jpeg'},
    'png':   {'type':'lossless','channels':'RGBA','depth':'8/16','alpha':True,'animation':False,'web':True,'mime':'image/png'},
    'gif':   {'type':'lossless/palette','channels':'indexed','depth':'8','alpha':True,'animation':True,'web':True,'mime':'image/gif','max_colors':256},
    'webp':  {'type':'lossy/lossless','channels':'RGBA','depth':'8','alpha':True,'animation':True,'web':True,'mime':'image/webp'},
    'avif':  {'type':'lossy/lossless','channels':'RGBA','depth':'8/10/12','alpha':True,'animation':True,'web':True,'mime':'image/avif'},
    'jxl':   {'type':'lossy/lossless','channels':'RGBA','depth':'8-32','alpha':True,'animation':True,'web':'partial','mime':'image/jxl'},
    'tiff':  {'type':'lossless','channels':'RGBA','depth':'8/16/32f','alpha':True,'animation':False,'web':False,'mime':'image/tiff'},
    'bmp':   {'type':'uncompressed','channels':'RGB/RGBA','depth':'1-32','alpha':True,'animation':False,'web':False,'mime':'image/bmp'},
    'svg':   {'type':'vector','channels':'N/A','depth':'N/A','alpha':True,'animation':True,'web':True,'mime':'image/svg+xml'},
    'heif':  {'type':'lossy/lossless','channels':'RGBA','depth':'8/10','alpha':True,'animation':True,'web':False,'mime':'image/heif'},
    'exr':   {'type':'lossless/lossy','channels':'RGBA+','depth':'16f/32f','alpha':True,'animation':False,'web':False,'mime':'image/x-exr','hdr':True},
    'raw':   {'type':'sensor data','channels':'Bayer/RGB','depth':'12-16','alpha':False,'animation':False,'web':False,'note':'Camera RAW (CR2/NEF/ARW)'},
    'ico':   {'type':'lossless','channels':'RGBA','depth':'8','alpha':True,'animation':False,'web':True,'mime':'image/x-icon','max_size':'256x256'},
    'psd':   {'type':'lossless','channels':'RGBA+layers','depth':'8/16/32','alpha':True,'animation':False,'web':False,'mime':'image/vnd.adobe.photoshop'},
}
f = formats.get(fmt.lower().replace('.',''))
return json.dumps(f) if f else json.dumps({'error':'unknown','known':list(formats.keys())})
$$ LANGUAGE plpython3u IMMUTABLE;
