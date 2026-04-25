-- ============================================================
-- SUBSTRATE LIBRARY: lib.signal
-- How the substrate perceives: transforms, filters, detection.
-- ============================================================

-- ===== FOURIER TRANSFORM =====

-- DFT (Discrete Fourier Transform) — returns magnitude spectrum
CREATE OR REPLACE FUNCTION substrate.dft_magnitude(samples FLOAT8[])
RETURNS FLOAT8[] AS $$
import math
N = len(samples)
mags = []
for k in range(N // 2 + 1):
    re = sum(samples[n] * math.cos(2*math.pi*k*n/N) for n in range(N))
    im = -sum(samples[n] * math.sin(2*math.pi*k*n/N) for n in range(N))
    mags.append(math.sqrt(re*re + im*im) / N)
return mags
$$ LANGUAGE plpython3u IMMUTABLE;

-- Dominant frequency from spectrum (index of max magnitude)
CREATE OR REPLACE FUNCTION substrate.dominant_freq_bin(magnitudes FLOAT8[])
RETURNS INT AS $$
if not magnitudes: return 0
return max(range(1, len(magnitudes)), key=lambda i: magnitudes[i])
$$ LANGUAGE plpython3u IMMUTABLE;

-- Frequency of a DFT bin
CREATE OR REPLACE FUNCTION substrate.bin_to_freq(bin_idx INT, n_samples INT, sample_rate FLOAT8)
RETURNS FLOAT8 AS $$ SELECT bin_idx::float8 * sample_rate / n_samples $$ LANGUAGE sql IMMUTABLE;

-- Spectral centroid: center of mass of spectrum
CREATE OR REPLACE FUNCTION substrate.spectral_centroid(magnitudes FLOAT8[], sample_rate FLOAT8 DEFAULT 1.0, n_samples INT DEFAULT 0)
RETURNS FLOAT8 AS $$
n = n_samples if n_samples > 0 else (len(magnitudes) - 1) * 2
total_mag = sum(magnitudes)
if total_mag == 0: return 0
weighted = sum(i * sample_rate / n * m for i, m in enumerate(magnitudes))
return weighted / total_mag
$$ LANGUAGE plpython3u IMMUTABLE;

-- Spectral rolloff: frequency below which X% of energy is contained
CREATE OR REPLACE FUNCTION substrate.spectral_rolloff(magnitudes FLOAT8[], rolloff_pct FLOAT8 DEFAULT 0.85)
RETURNS INT AS $$
total = sum(m*m for m in magnitudes)
if total == 0: return 0
running = 0
for i, m in enumerate(magnitudes):
    running += m * m
    if running >= rolloff_pct * total:
        return i
return len(magnitudes) - 1
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== CONVOLUTION =====

-- 1D convolution
CREATE OR REPLACE FUNCTION substrate.convolve(signal FLOAT8[], kernel FLOAT8[])
RETURNS FLOAT8[] AS $$
ns, nk = len(signal), len(kernel)
result = []
for i in range(ns + nk - 1):
    s = 0
    for j in range(nk):
        si = i - j
        if 0 <= si < ns:
            s += signal[si] * kernel[j]
    result.append(s)
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- Cross-correlation (unnormalized)
CREATE OR REPLACE FUNCTION substrate.cross_correlate(x FLOAT8[], y FLOAT8[])
RETURNS FLOAT8[] AS $$
nx, ny = len(x), len(y)
result = []
for lag in range(-(ny-1), nx):
    s = 0
    for j in range(ny):
        xi = lag + j
        if 0 <= xi < nx:
            s += x[xi] * y[j]
    result.append(s)
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- Autocorrelation (normalized)
CREATE OR REPLACE FUNCTION substrate.autocorrelation(x FLOAT8[], max_lag INT DEFAULT 0)
RETURNS FLOAT8[] AS $$
n = len(x)
if n == 0: return []
mx = sum(x) / n
var = sum((xi - mx)**2 for xi in x)
if var == 0: return [1.0] * (max_lag or n)
ml = max_lag if max_lag > 0 else n
result = []
for lag in range(ml):
    c = sum((x[i] - mx) * (x[i+lag] - mx) for i in range(n - lag)) / var
    result.append(c)
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== FILTERS =====

-- Moving average filter (low-pass)
CREATE OR REPLACE FUNCTION substrate.lowpass_ma(signal FLOAT8[], win INT DEFAULT 5)
RETURNS FLOAT8[] AS $$
result = []
for i in range(len(signal)):
    lo = max(0, i - win//2)
    hi = min(len(signal), i + win//2 + 1)
    result.append(sum(signal[lo:hi]) / (hi - lo))
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- High-pass filter (signal minus low-pass)
CREATE OR REPLACE FUNCTION substrate.highpass(signal FLOAT8[], win INT DEFAULT 5)
RETURNS FLOAT8[] AS $$
lp = []
for i in range(len(signal)):
    lo = max(0, i - win//2)
    hi = min(len(signal), i + win//2 + 1)
    lp.append(sum(signal[lo:hi]) / (hi - lo))
return [signal[i] - lp[i] for i in range(len(signal))]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Band-pass filter (low-pass minus lower low-pass)
CREATE OR REPLACE FUNCTION substrate.bandpass(signal FLOAT8[], low_win INT DEFAULT 3, high_win INT DEFAULT 15)
RETURNS FLOAT8[] AS $$
def ma(s, w):
    r = []
    for i in range(len(s)):
        lo = max(0, i - w//2)
        hi = min(len(s), i + w//2 + 1)
        r.append(sum(s[lo:hi]) / (hi - lo))
    return r
lp_narrow = ma(signal, low_win)
lp_wide = ma(signal, high_win)
return [lp_narrow[i] - lp_wide[i] for i in range(len(signal))]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Exponential smoothing filter
CREATE OR REPLACE FUNCTION substrate.exp_smooth(signal FLOAT8[], alpha FLOAT8 DEFAULT 0.3)
RETURNS FLOAT8[] AS $$
if not signal: return []
out = [signal[0]]
for i in range(1, len(signal)):
    out.append(alpha * signal[i] + (1 - alpha) * out[-1])
return out
$$ LANGUAGE plpython3u IMMUTABLE;

-- Median filter (noise removal)
CREATE OR REPLACE FUNCTION substrate.median_filter(signal FLOAT8[], win INT DEFAULT 5)
RETURNS FLOAT8[] AS $$
result = []
half = win // 2
for i in range(len(signal)):
    lo = max(0, i - half)
    hi = min(len(signal), i + half + 1)
    window = sorted(signal[lo:hi])
    result.append(window[len(window)//2])
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== WINDOWING =====

-- Hann window
CREATE OR REPLACE FUNCTION substrate.hann_window(n INT)
RETURNS FLOAT8[] AS $$
import math
return [0.5 * (1 - math.cos(2*math.pi*i/(n-1))) for i in range(n)]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Hamming window
CREATE OR REPLACE FUNCTION substrate.hamming_window(n INT)
RETURNS FLOAT8[] AS $$
import math
return [0.54 - 0.46 * math.cos(2*math.pi*i/(n-1)) for i in range(n)]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Apply window to signal
CREATE OR REPLACE FUNCTION substrate.apply_window(signal FLOAT8[], win FLOAT8[])
RETURNS FLOAT8[] AS $$
return [s * w for s, w in zip(signal, win)]
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== DETECTION =====

-- Peak detection: find local maxima above threshold
CREATE OR REPLACE FUNCTION substrate.find_peaks(signal FLOAT8[], threshold FLOAT8 DEFAULT 0, min_distance INT DEFAULT 1)
RETURNS INT[] AS $$
peaks = []
for i in range(1, len(signal) - 1):
    if signal[i] > signal[i-1] and signal[i] > signal[i+1] and signal[i] > threshold:
        if not peaks or (i - peaks[-1]) >= min_distance:
            peaks.append(i)
return peaks
$$ LANGUAGE plpython3u IMMUTABLE;

-- Zero-crossing rate: how often the signal changes sign
CREATE OR REPLACE FUNCTION substrate.zero_crossing_rate(signal FLOAT8[])
RETURNS FLOAT8 AS $$
if len(signal) < 2: return 0
crossings = sum(1 for i in range(1, len(signal)) if signal[i] * signal[i-1] < 0)
return crossings / (len(signal) - 1)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Signal-to-noise ratio (dB) from signal and noise power
CREATE OR REPLACE FUNCTION substrate.snr_db(signal_power FLOAT8, noise_power FLOAT8)
RETURNS FLOAT8 AS $$ SELECT 10 * log(signal_power / NULLIF(noise_power, 0)) $$ LANGUAGE sql IMMUTABLE;

-- SNR estimation from signal (assumes noise = deviation from smoothed)
CREATE OR REPLACE FUNCTION substrate.estimate_snr(signal FLOAT8[], smooth_win INT DEFAULT 10)
RETURNS FLOAT8 AS $$
import math
if len(signal) < smooth_win: return 0
# Smooth as signal estimate
smoothed = []
for i in range(len(signal)):
    lo = max(0, i - smooth_win//2)
    hi = min(len(signal), i + smooth_win//2 + 1)
    smoothed.append(sum(signal[lo:hi]) / (hi - lo))
signal_power = sum(s**2 for s in smoothed) / len(smoothed)
noise_power = sum((signal[i] - smoothed[i])**2 for i in range(len(signal))) / len(signal)
if noise_power == 0: return float('inf')
return 10 * math.log10(signal_power / noise_power)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Change point detection (CUSUM): detect when a signal shifts
CREATE OR REPLACE FUNCTION substrate.cusum_detect(signal FLOAT8[], threshold FLOAT8 DEFAULT 5.0, drift FLOAT8 DEFAULT 0.5)
RETURNS INT[] AS $$
n = len(signal)
if n < 2: return []
mu = sum(signal[:min(20, n)]) / min(20, n)
s_pos = s_neg = 0
changes = []
for i in range(n):
    s_pos = max(0, s_pos + signal[i] - mu - drift)
    s_neg = max(0, s_neg - signal[i] + mu - drift)
    if s_pos > threshold or s_neg > threshold:
        changes.append(i)
        s_pos = s_neg = 0
        mu = signal[i]
return changes
$$ LANGUAGE plpython3u IMMUTABLE;

-- Anomaly score: how many standard deviations from rolling mean
CREATE OR REPLACE FUNCTION substrate.anomaly_score(signal FLOAT8[], idx INT, lookback INT DEFAULT 20)
RETURNS FLOAT8 AS $$
import math
lo = max(0, idx - lookback)
window = signal[lo:idx]
if len(window) < 2: return 0
mu = sum(window) / len(window)
sigma = math.sqrt(sum((x - mu)**2 for x in window) / len(window))
if sigma == 0: return 0
return abs(signal[idx] - mu) / sigma
$$ LANGUAGE plpython3u IMMUTABLE;
