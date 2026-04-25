-- ============================================================
-- SUBSTRATE LIBRARY: lib.text
-- String metrics, similarity, hashing, and NLP primitives
-- ============================================================

-- Levenshtein edit distance
CREATE OR REPLACE FUNCTION substrate.levenshtein(a TEXT, b TEXT)
RETURNS INT AS $$
m, n = len(a), len(b)
dp = list(range(n+1))
for i in range(1, m+1):
    prev, dp[0] = dp[0], i
    for j in range(1, n+1):
        prev, dp[j] = dp[j], min(dp[j]+1, dp[j-1]+1, prev + (0 if a[i-1]==b[j-1] else 1))
return dp[n]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Jaro-Winkler similarity [0..1]
CREATE OR REPLACE FUNCTION substrate.jaro_winkler(s1 TEXT, s2 TEXT)
RETURNS FLOAT8 AS $$
if s1 == s2: return 1.0
l1, l2 = len(s1), len(s2)
if l1 == 0 or l2 == 0: return 0.0
window = max(l1, l2) // 2 - 1
m1 = [False]*l1; m2 = [False]*l2
matches = trans = 0
for i in range(l1):
    lo, hi = max(0, i-window), min(l2, i+window+1)
    for j in range(lo, hi):
        if m2[j] or s1[i] != s2[j]: continue
        m1[i] = m2[j] = True; matches += 1; break
if matches == 0: return 0.0
k = 0
for i in range(l1):
    if not m1[i]: continue
    while not m2[k]: k += 1
    if s1[i] != s2[k]: trans += 1
    k += 1
jaro = (matches/l1 + matches/l2 + (matches - trans/2)/matches) / 3
prefix = 0
for i in range(min(4, l1, l2)):
    if s1[i] == s2[i]: prefix += 1
    else: break
return jaro + prefix * 0.1 * (1 - jaro)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Dice coefficient (bigram similarity)
CREATE OR REPLACE FUNCTION substrate.dice(a TEXT, b TEXT)
RETURNS FLOAT8 AS $$
if len(a) < 2 or len(b) < 2: return 1.0 if a == b else 0.0
bg_a = set(a[i:i+2] for i in range(len(a)-1))
bg_b = set(b[i:i+2] for i in range(len(b)-1))
return 2 * len(bg_a & bg_b) / (len(bg_a) + len(bg_b))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Soundex
CREATE OR REPLACE FUNCTION substrate.soundex(word TEXT)
RETURNS TEXT AS $$
if not word: return ''
w = word.upper()
codes = {'B':'1','F':'1','P':'1','V':'1','C':'2','G':'2','J':'2','K':'2','Q':'2','S':'2','X':'2','Z':'2','D':'3','T':'3','L':'4','M':'5','N':'5','R':'6'}
result = w[0]
prev = codes.get(w[0], '0')
for c in w[1:]:
    code = codes.get(c, '0')
    if code != '0' and code != prev:
        result += code
    if c not in ('H','W'):
        prev = code
    if len(result) == 4: break
return result.ljust(4, '0')
$$ LANGUAGE plpython3u IMMUTABLE;

-- Tokenize (split on non-alphanumeric, lowercase, deduplicate)
CREATE OR REPLACE FUNCTION substrate.tokenize(input TEXT)
RETURNS TEXT[] AS $$
import re
tokens = re.findall(r'[a-zA-Z0-9]+', input.lower())
seen = set()
result = []
for t in tokens:
    if t not in seen:
        seen.add(t)
        result.append(t)
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- N-grams
CREATE OR REPLACE FUNCTION substrate.ngrams(input TEXT, n INT DEFAULT 2)
RETURNS TEXT[] AS $$
import re
tokens = re.findall(r'[a-zA-Z0-9]+', input.lower())
if len(tokens) < n: return [' '.join(tokens)] if tokens else []
return [' '.join(tokens[i:i+n]) for i in range(len(tokens)-n+1)]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Character n-grams (for fuzzy matching)
CREATE OR REPLACE FUNCTION substrate.char_ngrams(input TEXT, n INT DEFAULT 3)
RETURNS TEXT[] AS $$
s = input.lower()
if len(s) < n: return [s] if s else []
return list(set(s[i:i+n] for i in range(len(s)-n+1)))
$$ LANGUAGE plpython3u IMMUTABLE;

-- TF-IDF score for a term in a document given corpus stats
-- tf = term frequency in doc, df = document frequency, N = total docs
CREATE OR REPLACE FUNCTION substrate.tfidf(tf INT, df INT, total_docs INT)
RETURNS FLOAT8 AS $$
import math
if df == 0 or total_docs == 0: return 0
return tf * math.log(total_docs / df)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Cosine similarity of two float arrays (vectors)
CREATE OR REPLACE FUNCTION substrate.cosine_sim(a FLOAT8[], b FLOAT8[])
RETURNS FLOAT8 AS $$
import math
n = min(len(a), len(b))
dot = sum(a[i]*b[i] for i in range(n))
na = math.sqrt(sum(x*x for x in a[:n]))
nb = math.sqrt(sum(x*x for x in b[:n]))
if na == 0 or nb == 0: return 0
return dot / (na * nb)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Jaccard similarity of two text arrays (sets)
CREATE OR REPLACE FUNCTION substrate.jaccard(a TEXT[], b TEXT[])
RETURNS FLOAT8 AS $$
sa, sb = set(a), set(b)
union = sa | sb
if not union: return 1.0
return len(sa & sb) / len(union)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Hamming distance (equal-length strings)
CREATE OR REPLACE FUNCTION substrate.hamming(a TEXT, b TEXT)
RETURNS INT AS $$
return sum(c1 != c2 for c1, c2 in zip(a, b)) + abs(len(a)-len(b))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Slug (URL-safe string)
CREATE OR REPLACE FUNCTION substrate.slugify(input TEXT)
RETURNS TEXT AS $$
import re
s = input.lower().strip()
s = re.sub(r'[^\w\s-]', '', s)
s = re.sub(r'[\s_]+', '-', s)
return re.sub(r'-+', '-', s).strip('-')
$$ LANGUAGE plpython3u IMMUTABLE;

-- Truncate with ellipsis
CREATE OR REPLACE FUNCTION substrate.truncate_text(input TEXT, max_len INT DEFAULT 80)
RETURNS TEXT AS $$ SELECT CASE WHEN length(input) <= max_len THEN input ELSE left(input, max_len - 3) || '...' END $$ LANGUAGE sql IMMUTABLE;

-- Estimate token count (GPT-style ~4 chars/token)
CREATE OR REPLACE FUNCTION substrate.est_tokens(input TEXT)
RETURNS INT AS $$ SELECT GREATEST(1, (length(input) + 3) / 4) $$ LANGUAGE sql IMMUTABLE;

-- Extract all emails from text
CREATE OR REPLACE FUNCTION substrate.extract_emails(input TEXT)
RETURNS TEXT[] AS $$
import re
return re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', input)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Extract all URLs from text
CREATE OR REPLACE FUNCTION substrate.extract_urls(input TEXT)
RETURNS TEXT[] AS $$
import re
return re.findall(r'https?://[^\s<>"\')\]]+', input)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Extract all IPv4 addresses from text
CREATE OR REPLACE FUNCTION substrate.extract_ips(input TEXT)
RETURNS TEXT[] AS $$
import re
return re.findall(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', input)
$$ LANGUAGE plpython3u IMMUTABLE;
