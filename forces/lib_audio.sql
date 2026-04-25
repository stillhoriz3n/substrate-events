-- ============================================================
-- SUBSTRATE LIBRARY: lib.audio
-- Audio engineering: dB, frequency, sampling, loudness, formats
-- ============================================================

-- ===== DECIBEL CONVERSIONS =====

-- Linear amplitude to dB (ref=1.0)
CREATE OR REPLACE FUNCTION substrate.amp_to_db(amplitude FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 20 * log(amplitude) $$ LANGUAGE sql IMMUTABLE;

-- dB to linear amplitude
CREATE OR REPLACE FUNCTION substrate.db_to_amp(db FLOAT8)
RETURNS FLOAT8 AS $$ SELECT power(10, db / 20.0) $$ LANGUAGE sql IMMUTABLE;

-- Power ratio to dB
CREATE OR REPLACE FUNCTION substrate.power_to_db(power_ratio FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 10 * log(power_ratio) $$ LANGUAGE sql IMMUTABLE;

-- dB to power ratio
CREATE OR REPLACE FUNCTION substrate.db_to_power(db FLOAT8)
RETURNS FLOAT8 AS $$ SELECT power(10, db / 10.0) $$ LANGUAGE sql IMMUTABLE;

-- Sum two dB values (incoherent addition)
CREATE OR REPLACE FUNCTION substrate.db_sum(db1 FLOAT8, db2 FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 10 * log(power(10, db1/10.0) + power(10, db2/10.0)) $$ LANGUAGE sql IMMUTABLE;

-- dBFS from sample value and bit depth (full-scale reference)
CREATE OR REPLACE FUNCTION substrate.dbfs(sample_val FLOAT8, bit_depth INT DEFAULT 16)
RETURNS FLOAT8 AS $$
import math
peak = 2**(bit_depth - 1) - 1
if sample_val <= 0: return -float('inf')
return 20 * math.log10(abs(sample_val) / peak)
$$ LANGUAGE plpython3u IMMUTABLE;

-- dBu to dBV (dBu = dBV + 2.2)
CREATE OR REPLACE FUNCTION substrate.dbu_to_dbv(dbu FLOAT8)
RETURNS FLOAT8 AS $$ SELECT dbu - 2.2 $$ LANGUAGE sql IMMUTABLE;

-- dBV to dBu
CREATE OR REPLACE FUNCTION substrate.dbv_to_dbu(dbv FLOAT8)
RETURNS FLOAT8 AS $$ SELECT dbv + 2.2 $$ LANGUAGE sql IMMUTABLE;

-- dBm to watts
CREATE OR REPLACE FUNCTION substrate.dbm_to_watts(dbm FLOAT8)
RETURNS FLOAT8 AS $$ SELECT power(10, dbm / 10.0) / 1000.0 $$ LANGUAGE sql IMMUTABLE;

-- Watts to dBm
CREATE OR REPLACE FUNCTION substrate.watts_to_dbm(watts FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 10 * log(watts * 1000.0) $$ LANGUAGE sql IMMUTABLE;

-- ===== FREQUENCY / PITCH =====

-- MIDI note to frequency (A4 = 69 = 440 Hz)
CREATE OR REPLACE FUNCTION substrate.midi_to_freq(midi_note INT, a4_hz FLOAT8 DEFAULT 440.0)
RETURNS FLOAT8 AS $$ SELECT a4_hz * power(2, (midi_note - 69.0) / 12.0) $$ LANGUAGE sql IMMUTABLE;

-- Frequency to MIDI note (nearest)
CREATE OR REPLACE FUNCTION substrate.freq_to_midi(freq FLOAT8, a4_hz FLOAT8 DEFAULT 440.0)
RETURNS INT AS $$ SELECT round(69 + 12 * log(freq / a4_hz) / log(2.0))::int $$ LANGUAGE sql IMMUTABLE;

-- MIDI note to note name
CREATE OR REPLACE FUNCTION substrate.midi_to_name(midi_note INT)
RETURNS TEXT AS $$
notes = ['C','C#','D','D#','E','F','F#','G','G#','A','A#','B']
octave = (midi_note // 12) - 1
return f'{notes[midi_note % 12]}{octave}'
$$ LANGUAGE plpython3u IMMUTABLE;

-- Note name to MIDI number (e.g., 'A4' -> 69, 'C#3' -> 49)
CREATE OR REPLACE FUNCTION substrate.name_to_midi(note_name TEXT)
RETURNS INT AS $$
import re
m = re.match(r'^([A-Ga-g])(#|b)?(-?\d+)$', note_name.strip())
if not m: return None
note_map = {'C':0,'D':2,'E':4,'F':5,'G':7,'A':9,'B':11}
base = note_map.get(m.group(1).upper(), 0)
if m.group(2) == '#': base += 1
elif m.group(2) == 'b': base -= 1
octave = int(m.group(3))
return (octave + 1) * 12 + base
$$ LANGUAGE plpython3u IMMUTABLE;

-- Frequency to wavelength (meters, speed of sound in air at 20°C = 343 m/s)
CREATE OR REPLACE FUNCTION substrate.freq_to_wavelength(freq FLOAT8, speed FLOAT8 DEFAULT 343.0)
RETURNS FLOAT8 AS $$ SELECT speed / freq $$ LANGUAGE sql IMMUTABLE;

-- Cents between two frequencies
CREATE OR REPLACE FUNCTION substrate.cents(freq1 FLOAT8, freq2 FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 1200 * log(freq2 / freq1) / log(2.0) $$ LANGUAGE sql IMMUTABLE;

-- Frequency of nth harmonic
CREATE OR REPLACE FUNCTION substrate.harmonic(fundamental FLOAT8, n INT)
RETURNS FLOAT8 AS $$ SELECT fundamental * n $$ LANGUAGE sql IMMUTABLE;

-- Equal temperament interval ratio (n semitones)
CREATE OR REPLACE FUNCTION substrate.semitone_ratio(n_semitones INT)
RETURNS FLOAT8 AS $$ SELECT power(2, n_semitones / 12.0) $$ LANGUAGE sql IMMUTABLE;

-- BPM to milliseconds per beat
CREATE OR REPLACE FUNCTION substrate.bpm_to_ms(bpm FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 60000.0 / bpm $$ LANGUAGE sql IMMUTABLE;

-- BPM to samples per beat
CREATE OR REPLACE FUNCTION substrate.bpm_to_samples(bpm FLOAT8, sample_rate INT DEFAULT 44100)
RETURNS FLOAT8 AS $$ SELECT (60.0 / bpm) * sample_rate $$ LANGUAGE sql IMMUTABLE;

-- Delay time in ms for musical subdivision (e.g., quarter=1, eighth=0.5)
CREATE OR REPLACE FUNCTION substrate.musical_delay_ms(bpm FLOAT8, subdivision FLOAT8 DEFAULT 1.0)
RETURNS FLOAT8 AS $$ SELECT (60000.0 / bpm) * subdivision $$ LANGUAGE sql IMMUTABLE;

-- ===== SAMPLING & DIGITAL AUDIO =====

-- Nyquist frequency from sample rate
CREATE OR REPLACE FUNCTION substrate.nyquist(sample_rate INT)
RETURNS INT AS $$ SELECT sample_rate / 2 $$ LANGUAGE sql IMMUTABLE;

-- Samples to time (seconds)
CREATE OR REPLACE FUNCTION substrate.samples_to_sec(n_samples BIGINT, sample_rate INT DEFAULT 44100)
RETURNS FLOAT8 AS $$ SELECT n_samples::float8 / sample_rate $$ LANGUAGE sql IMMUTABLE;

-- Time to samples
CREATE OR REPLACE FUNCTION substrate.sec_to_samples(seconds FLOAT8, sample_rate INT DEFAULT 44100)
RETURNS BIGINT AS $$ SELECT (seconds * sample_rate)::bigint $$ LANGUAGE sql IMMUTABLE;

-- PCM audio file size (bytes): duration_sec * sample_rate * channels * (bit_depth/8)
CREATE OR REPLACE FUNCTION substrate.pcm_size(duration_sec FLOAT8, sample_rate INT DEFAULT 44100, channels INT DEFAULT 2, bit_depth INT DEFAULT 16)
RETURNS BIGINT AS $$ SELECT (duration_sec * sample_rate * channels * (bit_depth / 8))::bigint $$ LANGUAGE sql IMMUTABLE;

-- WAV file size (PCM + 44-byte header)
CREATE OR REPLACE FUNCTION substrate.wav_size(duration_sec FLOAT8, sample_rate INT DEFAULT 44100, channels INT DEFAULT 2, bit_depth INT DEFAULT 16)
RETURNS BIGINT AS $$ SELECT 44 + (duration_sec * sample_rate * channels * (bit_depth / 8))::bigint $$ LANGUAGE sql IMMUTABLE;

-- Audio bitrate (kbps) for PCM
CREATE OR REPLACE FUNCTION substrate.pcm_bitrate(sample_rate INT DEFAULT 44100, channels INT DEFAULT 2, bit_depth INT DEFAULT 16)
RETURNS FLOAT8 AS $$ SELECT (sample_rate::float8 * channels * bit_depth) / 1000.0 $$ LANGUAGE sql IMMUTABLE;

-- Duration from file size and bitrate (kbps)
CREATE OR REPLACE FUNCTION substrate.audio_duration_from_size(file_bytes BIGINT, bitrate_kbps FLOAT8)
RETURNS FLOAT8 AS $$ SELECT (file_bytes * 8.0) / (bitrate_kbps * 1000.0) $$ LANGUAGE sql IMMUTABLE;

-- Sample rate conversion ratio
CREATE OR REPLACE FUNCTION substrate.src_ratio(source_rate INT, target_rate INT)
RETURNS FLOAT8 AS $$ SELECT target_rate::float8 / source_rate $$ LANGUAGE sql IMMUTABLE;

-- Dynamic range in dB from bit depth
CREATE OR REPLACE FUNCTION substrate.bit_depth_dynamic_range(bit_depth INT)
RETURNS FLOAT8 AS $$ SELECT 6.02 * bit_depth + 1.76 $$ LANGUAGE sql IMMUTABLE;

-- ===== LOUDNESS & METERING =====

-- RMS of sample array
CREATE OR REPLACE FUNCTION substrate.rms(samples FLOAT8[])
RETURNS FLOAT8 AS $$
import math
if not samples: return 0
return math.sqrt(sum(s*s for s in samples) / len(samples))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Peak-to-RMS ratio (crest factor) in dB
CREATE OR REPLACE FUNCTION substrate.crest_factor_db(peak FLOAT8, rms_val FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 20 * log(peak / NULLIF(rms_val, 0)) $$ LANGUAGE sql IMMUTABLE;

-- A-weighting approximation at given frequency (relative dB)
CREATE OR REPLACE FUNCTION substrate.a_weight(freq FLOAT8)
RETURNS FLOAT8 AS $$
import math
f2 = freq * freq
num = 12194.0**2 * f2 * f2
den = (f2 + 20.6**2) * math.sqrt((f2 + 107.7**2) * (f2 + 737.9**2)) * (f2 + 12194.0**2)
if den == 0: return -float('inf')
ra = num / den
return 20 * math.log10(ra) + 2.0
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== FORMAT SPECS =====

-- Common audio format info lookup
CREATE OR REPLACE FUNCTION substrate.audio_format_info(fmt TEXT)
RETURNS JSONB AS $$
import json
formats = {
    'wav':  {'container':'RIFF','codec':'PCM','lossy':False,'typical_bitrate':'1411 kbps','ext':'.wav'},
    'flac': {'container':'FLAC','codec':'FLAC','lossy':False,'typical_bitrate':'800-1000 kbps','ext':'.flac'},
    'alac': {'container':'M4A','codec':'ALAC','lossy':False,'typical_bitrate':'800-1000 kbps','ext':'.m4a'},
    'mp3':  {'container':'MPEG','codec':'MP3/LAME','lossy':True,'typical_bitrate':'128-320 kbps','ext':'.mp3'},
    'aac':  {'container':'M4A/ADTS','codec':'AAC-LC','lossy':True,'typical_bitrate':'128-256 kbps','ext':'.m4a'},
    'ogg':  {'container':'OGG','codec':'Vorbis','lossy':True,'typical_bitrate':'80-320 kbps','ext':'.ogg'},
    'opus': {'container':'OGG/WebM','codec':'Opus','lossy':True,'typical_bitrate':'32-256 kbps','ext':'.opus'},
    'wma':  {'container':'ASF','codec':'WMA','lossy':True,'typical_bitrate':'128-320 kbps','ext':'.wma'},
    'aiff': {'container':'AIFF','codec':'PCM','lossy':False,'typical_bitrate':'1411 kbps','ext':'.aiff'},
    'dsd':  {'container':'DFF/DSF','codec':'DSD','lossy':False,'typical_bitrate':'2822-11289 kbps','ext':'.dsf'},
    'ac3':  {'container':'AC3','codec':'Dolby Digital','lossy':True,'typical_bitrate':'384-640 kbps','ext':'.ac3'},
    'eac3': {'container':'EC3','codec':'Dolby Digital Plus','lossy':True,'typical_bitrate':'256-1024 kbps','ext':'.eac3'},
    'atmos':{'container':'EC3/TrueHD','codec':'Dolby Atmos','lossy':False,'typical_bitrate':'variable','ext':'.eac3'},
    'dts':  {'container':'DTS','codec':'DTS Core','lossy':True,'typical_bitrate':'768-1536 kbps','ext':'.dts'},
    'dtshd':{'container':'DTS-HD','codec':'DTS-HD MA','lossy':False,'typical_bitrate':'variable','ext':'.dtshd'},
}
f = formats.get(fmt.lower().replace('-','').replace(' ',''))
return json.dumps(f) if f else json.dumps({'error': f'unknown format: {fmt}', 'known': list(formats.keys())})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Standard sample rates
CREATE OR REPLACE FUNCTION substrate.standard_sample_rates()
RETURNS INT[] AS $$ SELECT ARRAY[8000, 11025, 16000, 22050, 32000, 44100, 48000, 88200, 96000, 176400, 192000, 352800, 384000] $$ LANGUAGE sql IMMUTABLE;
