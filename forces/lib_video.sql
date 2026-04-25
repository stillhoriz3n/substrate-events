-- ============================================================
-- SUBSTRATE LIBRARY: lib.video
-- Video engineering: timecode, framerate, resolution, color, bitrate
-- ============================================================

-- ===== SMPTE TIMECODE =====

-- Frame number to SMPTE timecode string (HH:MM:SS:FF)
CREATE OR REPLACE FUNCTION substrate.frames_to_tc(total_frames BIGINT, fps FLOAT8 DEFAULT 24.0)
RETURNS TEXT AS $$
f = int(total_frames)
ifps = round(fps)
frames = f % ifps
secs = (f // ifps) % 60
mins = (f // (ifps * 60)) % 60
hrs = f // (ifps * 3600)
return f'{hrs:02d}:{mins:02d}:{secs:02d}:{frames:02d}'
$$ LANGUAGE plpython3u IMMUTABLE;

-- SMPTE timecode string to frame number
CREATE OR REPLACE FUNCTION substrate.tc_to_frames(tc TEXT, fps FLOAT8 DEFAULT 24.0)
RETURNS BIGINT AS $$
parts = tc.strip().replace(';',':').split(':')
if len(parts) != 4: return None
h, m, s, f = [int(p) for p in parts]
ifps = round(fps)
return h * ifps * 3600 + m * ifps * 60 + s * ifps + f
$$ LANGUAGE plpython3u IMMUTABLE;

-- Timecode to seconds
CREATE OR REPLACE FUNCTION substrate.tc_to_sec(tc TEXT, fps FLOAT8 DEFAULT 24.0)
RETURNS FLOAT8 AS $$
parts = tc.strip().replace(';',':').split(':')
if len(parts) != 4: return None
h, m, s, f = [int(p) for p in parts]
return h * 3600.0 + m * 60.0 + s + f / fps
$$ LANGUAGE plpython3u IMMUTABLE;

-- Seconds to SMPTE timecode
CREATE OR REPLACE FUNCTION substrate.sec_to_tc(total_sec FLOAT8, fps FLOAT8 DEFAULT 24.0)
RETURNS TEXT AS $$
ifps = round(fps)
total_frames = round(total_sec * fps)
frames = int(total_frames) % ifps
secs = (int(total_frames) // ifps) % 60
mins = (int(total_frames) // (ifps * 60)) % 60
hrs = int(total_frames) // (ifps * 3600)
return f'{hrs:02d}:{mins:02d}:{secs:02d}:{frames:02d}'
$$ LANGUAGE plpython3u IMMUTABLE;

-- Drop-frame timecode offset (29.97fps NTSC)
CREATE OR REPLACE FUNCTION substrate.drop_frame_offset(total_frames BIGINT)
RETURNS BIGINT AS $$
# 29.97fps drops 2 frames every minute except every 10th minute
minutes = total_frames // 17982  # frames per 10 min at 29.97
remaining = total_frames % 17982
extra_drops = 0
if remaining > 2:
    extra_drops = 2 * ((remaining - 2) // 1798 + 1)
    extra_drops = min(extra_drops, 18)  # max 9 minutes of drops per 10min
return total_frames + minutes * 18 + extra_drops
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== FRAMERATE =====

-- Common framerates lookup
CREATE OR REPLACE FUNCTION substrate.framerate_info(name TEXT)
RETURNS JSONB AS $$
import json
rates = {
    'film':     {'fps':24.0,'exact':'24/1','standard':'SMPTE 24p'},
    'ntsc':     {'fps':29.97,'exact':'30000/1001','standard':'SMPTE 29.97i/p','drop_frame':True},
    'ntsc_film':{'fps':23.976,'exact':'24000/1001','standard':'SMPTE 23.976p'},
    'pal':      {'fps':25.0,'exact':'25/1','standard':'EBU 25i/p'},
    '30p':      {'fps':30.0,'exact':'30/1','standard':'SMPTE 30p'},
    '50p':      {'fps':50.0,'exact':'50/1','standard':'EBU 50p'},
    '59.94':    {'fps':59.94,'exact':'60000/1001','standard':'SMPTE 59.94p'},
    '60p':      {'fps':60.0,'exact':'60/1','standard':'SMPTE 60p'},
    '120p':     {'fps':120.0,'exact':'120/1','standard':'HFR 120p'},
    'hfr48':    {'fps':48.0,'exact':'48/1','standard':'HFR 48p'},
}
r = rates.get(name.lower().replace(' ','_'))
return json.dumps(r) if r else json.dumps({'error':'unknown','known':list(rates.keys())})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Frame duration in ms
CREATE OR REPLACE FUNCTION substrate.frame_duration_ms(fps FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 1000.0 / fps $$ LANGUAGE sql IMMUTABLE;

-- Total frames from duration and fps
CREATE OR REPLACE FUNCTION substrate.duration_to_frames(seconds FLOAT8, fps FLOAT8 DEFAULT 24.0)
RETURNS BIGINT AS $$ SELECT (seconds * fps)::bigint $$ LANGUAGE sql IMMUTABLE;

-- ===== RESOLUTION & ASPECT RATIO =====

-- Pixel count
CREATE OR REPLACE FUNCTION substrate.pixel_count(width INT, height INT)
RETURNS BIGINT AS $$ SELECT (width::bigint * height) $$ LANGUAGE sql IMMUTABLE;

-- Megapixels
CREATE OR REPLACE FUNCTION substrate.megapixels(width INT, height INT)
RETURNS FLOAT8 AS $$ SELECT (width::float8 * height) / 1000000.0 $$ LANGUAGE sql IMMUTABLE;

-- Aspect ratio as simplified string (e.g., "16:9")
CREATE OR REPLACE FUNCTION substrate.aspect_ratio(width INT, height INT)
RETURNS TEXT AS $$
def gcd(a, b):
    while b: a, b = b, a % b
    return a
g = gcd(width, height)
return f'{width//g}:{height//g}'
$$ LANGUAGE plpython3u IMMUTABLE;

-- Display aspect ratio (DAR) as float
CREATE OR REPLACE FUNCTION substrate.dar(width INT, height INT)
RETURNS FLOAT8 AS $$ SELECT width::float8 / height $$ LANGUAGE sql IMMUTABLE;

-- Width from height and aspect ratio
CREATE OR REPLACE FUNCTION substrate.width_from_ar(height INT, ar_w INT DEFAULT 16, ar_h INT DEFAULT 9)
RETURNS INT AS $$ SELECT ((height * ar_w / ar_h + 1) / 2 * 2)::int $$ LANGUAGE sql IMMUTABLE;

-- Height from width and aspect ratio
CREATE OR REPLACE FUNCTION substrate.height_from_ar(width INT, ar_w INT DEFAULT 16, ar_h INT DEFAULT 9)
RETURNS INT AS $$ SELECT ((width * ar_h / ar_w + 1) / 2 * 2)::int $$ LANGUAGE sql IMMUTABLE;

-- Standard resolution lookup
CREATE OR REPLACE FUNCTION substrate.resolution_info(name TEXT)
RETURNS JSONB AS $$
import json
resolutions = {
    'sd':       {'width':720,'height':480,'name':'SD (NTSC)','pixels':'345.6K'},
    'pal':      {'width':720,'height':576,'name':'SD (PAL)','pixels':'414.7K'},
    '720p':     {'width':1280,'height':720,'name':'HD 720p','pixels':'921.6K'},
    '1080p':    {'width':1920,'height':1080,'name':'Full HD','pixels':'2.07M'},
    '1080i':    {'width':1920,'height':1080,'name':'Full HD Interlaced','pixels':'2.07M'},
    '2k':       {'width':2048,'height':1080,'name':'DCI 2K','pixels':'2.21M'},
    '1440p':    {'width':2560,'height':1440,'name':'QHD / 2K','pixels':'3.69M'},
    '4k':       {'width':3840,'height':2160,'name':'UHD 4K','pixels':'8.29M'},
    'dci4k':    {'width':4096,'height':2160,'name':'DCI 4K','pixels':'8.85M'},
    '5k':       {'width':5120,'height':2880,'name':'5K','pixels':'14.75M'},
    '8k':       {'width':7680,'height':4320,'name':'UHD 8K','pixels':'33.18M'},
    'dci8k':    {'width':8192,'height':4320,'name':'DCI 8K','pixels':'35.39M'},
    'imax':     {'width':5616,'height':4096,'name':'IMAX Digital','pixels':'23.00M'},
}
r = resolutions.get(name.lower().replace(' ',''))
return json.dumps(r) if r else json.dumps({'error':'unknown','known':list(resolutions.keys())})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Scale resolution maintaining aspect ratio (fit within max_w x max_h)
CREATE OR REPLACE FUNCTION substrate.fit_resolution(src_w INT, src_h INT, max_w INT, max_h INT)
RETURNS INT[] AS $$
scale = min(max_w / src_w, max_h / src_h)
w = int(src_w * scale) // 2 * 2  # round to even
h = int(src_h * scale) // 2 * 2
return [w, h]
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== BITRATE & STORAGE =====

-- Video bitrate estimate (Mbps) — rule of thumb for H.264
CREATE OR REPLACE FUNCTION substrate.h264_bitrate_mbps(width INT, height INT, fps FLOAT8, quality TEXT DEFAULT 'medium')
RETURNS FLOAT8 AS $$
pixels_per_sec = width * height * fps
# bits per pixel (bpp) ranges by quality
bpp = {'low': 0.04, 'medium': 0.07, 'high': 0.10, 'broadcast': 0.14, 'lossless': 0.5}
b = bpp.get(quality.lower(), 0.07)
return (pixels_per_sec * b) / 1e6
$$ LANGUAGE plpython3u IMMUTABLE;

-- H.265 bitrate estimate (roughly 50% of H.264 at same quality)
CREATE OR REPLACE FUNCTION substrate.h265_bitrate_mbps(width INT, height INT, fps FLOAT8, quality TEXT DEFAULT 'medium')
RETURNS FLOAT8 AS $$
pixels_per_sec = width * height * fps
bpp = {'low': 0.02, 'medium': 0.035, 'high': 0.05, 'broadcast': 0.07, 'lossless': 0.25}
b = bpp.get(quality.lower(), 0.035)
return (pixels_per_sec * b) / 1e6
$$ LANGUAGE plpython3u IMMUTABLE;

-- AV1 bitrate estimate (roughly 30% less than H.265)
CREATE OR REPLACE FUNCTION substrate.av1_bitrate_mbps(width INT, height INT, fps FLOAT8, quality TEXT DEFAULT 'medium')
RETURNS FLOAT8 AS $$
pixels_per_sec = width * height * fps
bpp = {'low': 0.014, 'medium': 0.025, 'high': 0.035, 'broadcast': 0.05}
b = bpp.get(quality.lower(), 0.025)
return (pixels_per_sec * b) / 1e6
$$ LANGUAGE plpython3u IMMUTABLE;

-- Video file size estimate (MB): bitrate_mbps * duration_sec / 8
CREATE OR REPLACE FUNCTION substrate.video_size_mb(bitrate_mbps FLOAT8, duration_sec FLOAT8)
RETURNS FLOAT8 AS $$ SELECT bitrate_mbps * duration_sec / 8.0 $$ LANGUAGE sql IMMUTABLE;

-- Total stream bitrate (video + audio)
CREATE OR REPLACE FUNCTION substrate.total_bitrate_mbps(video_mbps FLOAT8, audio_kbps FLOAT8 DEFAULT 192)
RETURNS FLOAT8 AS $$ SELECT video_mbps + audio_kbps / 1000.0 $$ LANGUAGE sql IMMUTABLE;

-- Storage hours per TB at given total bitrate (Mbps)
CREATE OR REPLACE FUNCTION substrate.hours_per_tb(bitrate_mbps FLOAT8)
RETURNS FLOAT8 AS $$ SELECT (1099511627776.0 * 8) / (bitrate_mbps * 1e6 * 3600) $$ LANGUAGE sql IMMUTABLE;

-- ===== COLOR =====

-- RGB to hex string
CREATE OR REPLACE FUNCTION substrate.rgb_to_hex(r INT, g INT, b INT)
RETURNS TEXT AS $$ SELECT '#' || lpad(to_hex(r), 2, '0') || lpad(to_hex(g), 2, '0') || lpad(to_hex(b), 2, '0') $$ LANGUAGE sql IMMUTABLE;

-- Hex to RGB array
CREATE OR REPLACE FUNCTION substrate.hex_to_rgb(hex TEXT)
RETURNS INT[] AS $$
h = hex.lstrip('#')
return [int(h[i:i+2], 16) for i in (0, 2, 4)]
$$ LANGUAGE plpython3u IMMUTABLE;

-- RGB to HSL
CREATE OR REPLACE FUNCTION substrate.rgb_to_hsl(r INT, g INT, b INT)
RETURNS FLOAT8[] AS $$
rf, gf, bf = r/255.0, g/255.0, b/255.0
mx, mn = max(rf,gf,bf), min(rf,gf,bf)
l = (mx+mn)/2
if mx == mn:
    return [0, 0, round(l,4)]
d = mx - mn
s = d/(2-mx-mn) if l > 0.5 else d/(mx+mn)
if mx == rf: h = ((gf-bf)/d + (6 if gf<bf else 0)) / 6
elif mx == gf: h = ((bf-rf)/d + 2) / 6
else: h = ((rf-gf)/d + 4) / 6
return [round(h*360,2), round(s,4), round(l,4)]
$$ LANGUAGE plpython3u IMMUTABLE;

-- sRGB to linear (gamma decode)
CREATE OR REPLACE FUNCTION substrate.srgb_to_linear(val FLOAT8)
RETURNS FLOAT8 AS $$
if val <= 0.04045: return val / 12.92
return ((val + 0.055) / 1.055) ** 2.4
$$ LANGUAGE plpython3u IMMUTABLE;

-- Linear to sRGB (gamma encode)
CREATE OR REPLACE FUNCTION substrate.linear_to_srgb(val FLOAT8)
RETURNS FLOAT8 AS $$
if val <= 0.0031308: return val * 12.92
return 1.055 * (val ** (1/2.4)) - 0.055
$$ LANGUAGE plpython3u IMMUTABLE;

-- Relative luminance (sRGB input 0-255)
CREATE OR REPLACE FUNCTION substrate.luminance(r INT, g INT, b INT)
RETURNS FLOAT8 AS $$
def linear(v):
    v = v / 255.0
    return v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4
return 0.2126 * linear(r) + 0.7152 * linear(g) + 0.0722 * linear(b)
$$ LANGUAGE plpython3u IMMUTABLE;

-- WCAG contrast ratio between two colors (each as [R,G,B])
CREATE OR REPLACE FUNCTION substrate.contrast_ratio(r1 INT, g1 INT, b1 INT, r2 INT, g2 INT, b2 INT)
RETURNS FLOAT8 AS $$
def lum(r,g,b):
    def lin(v):
        v = v / 255.0
        return v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4
    return 0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(b)
l1, l2 = lum(r1,g1,b1), lum(r2,g2,b2)
lighter, darker = max(l1,l2), min(l1,l2)
return (lighter + 0.05) / (darker + 0.05)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Color space info
CREATE OR REPLACE FUNCTION substrate.color_space_info(name TEXT)
RETURNS JSONB AS $$
import json
spaces = {
    'rec709':  {'gamut':'sRGB/Rec.709','transfer':'BT.1886','white_point':'D65','bit_depth':'8/10','use':'HD broadcast'},
    'rec2020': {'gamut':'Rec.2020','transfer':'PQ/HLG','white_point':'D65','bit_depth':'10/12','use':'UHD/HDR'},
    'dci-p3':  {'gamut':'DCI-P3','transfer':'Gamma 2.6','white_point':'DCI','bit_depth':'12','use':'Digital cinema'},
    'display-p3':{'gamut':'Display P3','transfer':'sRGB','white_point':'D65','bit_depth':'8/10','use':'Apple/wide gamut'},
    'srgb':    {'gamut':'sRGB','transfer':'sRGB (~2.2)','white_point':'D65','bit_depth':'8','use':'Web/general'},
    'aces':    {'gamut':'ACES AP0','transfer':'Linear','white_point':'D60','bit_depth':'16f/32f','use':'VFX pipeline'},
    'acescg':  {'gamut':'ACES AP1','transfer':'Linear','white_point':'D60','bit_depth':'16f','use':'CG rendering'},
}
s = spaces.get(name.lower().replace(' ','-').replace('_','-'))
return json.dumps(s) if s else json.dumps({'error':'unknown','known':list(spaces.keys())})
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== GOP / ENCODING STRUCTURE =====

-- I-frame interval in seconds from GOP size and fps
CREATE OR REPLACE FUNCTION substrate.gop_interval_sec(gop_size INT, fps FLOAT8)
RETURNS FLOAT8 AS $$ SELECT gop_size / fps $$ LANGUAGE sql IMMUTABLE;

-- Recommended GOP size (typically 2x fps for streaming)
CREATE OR REPLACE FUNCTION substrate.recommended_gop(fps FLOAT8, target TEXT DEFAULT 'streaming')
RETURNS INT AS $$
targets = {'streaming': 2.0, 'broadcast': 0.5, 'archive': 10.0, 'low_latency': 0.5}
multiplier = targets.get(target.lower(), 2.0)
return round(fps * multiplier)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Video codec info
CREATE OR REPLACE FUNCTION substrate.video_codec_info(codec TEXT)
RETURNS JSONB AS $$
import json
codecs = {
    'h264':    {'name':'H.264/AVC','profile':'Baseline/Main/High','container':['MP4','MKV','TS'],'hw_decode':'universal','year':2003},
    'h265':    {'name':'H.265/HEVC','profile':'Main/Main10','container':['MP4','MKV','TS'],'hw_decode':'2015+','year':2013},
    'av1':     {'name':'AV1','profile':'Main/High','container':['MP4','MKV','WebM'],'hw_decode':'2022+','year':2018},
    'vp8':     {'name':'VP8','profile':'N/A','container':['WebM'],'hw_decode':'limited','year':2010},
    'vp9':     {'name':'VP9','profile':'0/2','container':['WebM','MP4'],'hw_decode':'2017+','year':2013},
    'prores':  {'name':'Apple ProRes','profile':'Proxy/LT/422/HQ/4444/XQ','container':['MOV'],'hw_decode':'Apple','year':2007},
    'dnxhr':   {'name':'Avid DNxHR','profile':'LB/SQ/HQ/HQX/444','container':['MXF','MOV'],'hw_decode':'limited','year':2015},
    'mjpeg':   {'name':'Motion JPEG','profile':'N/A','container':['AVI','MOV'],'hw_decode':'universal','year':1992},
    'mpeg2':   {'name':'MPEG-2','profile':'Main','container':['TS','MPG','VOB'],'hw_decode':'universal','year':1995},
    'vvc':     {'name':'H.266/VVC','profile':'Main10','container':['MP4'],'hw_decode':'2025+','year':2020},
}
c = codecs.get(codec.lower().replace('.','').replace('-','').replace('/','').replace(' ',''))
return json.dumps(c) if c else json.dumps({'error':'unknown','known':list(codecs.keys())})
$$ LANGUAGE plpython3u IMMUTABLE;
