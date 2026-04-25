-- ============================================================
-- SUBSTRATE LIBRARY: lib.crypto
-- Hashing, encoding, checksums, UUID generation
-- ============================================================

-- SHA-256 of text
CREATE OR REPLACE FUNCTION substrate.sha256(input TEXT)
RETURNS TEXT AS $$
import hashlib
return hashlib.sha256(input.encode()).hexdigest()
$$ LANGUAGE plpython3u IMMUTABLE;

-- SHA-512 of text
CREATE OR REPLACE FUNCTION substrate.sha512(input TEXT)
RETURNS TEXT AS $$
import hashlib
return hashlib.sha512(input.encode()).hexdigest()
$$ LANGUAGE plpython3u IMMUTABLE;

-- MD5 of text (non-crypto use: checksums, dedup keys)
CREATE OR REPLACE FUNCTION substrate.md5_hex(input TEXT)
RETURNS TEXT AS $$
import hashlib
return hashlib.md5(input.encode()).hexdigest()
$$ LANGUAGE plpython3u IMMUTABLE;

-- CRC32
CREATE OR REPLACE FUNCTION substrate.crc32(input TEXT)
RETURNS BIGINT AS $$
import binascii
return binascii.crc32(input.encode()) & 0xFFFFFFFF
$$ LANGUAGE plpython3u IMMUTABLE;

-- HMAC-SHA256
CREATE OR REPLACE FUNCTION substrate.hmac_sha256(key TEXT, message TEXT)
RETURNS TEXT AS $$
import hmac, hashlib
return hmac.new(key.encode(), message.encode(), hashlib.sha256).hexdigest()
$$ LANGUAGE plpython3u IMMUTABLE;

-- Base64 encode
CREATE OR REPLACE FUNCTION substrate.b64_encode(input TEXT)
RETURNS TEXT AS $$
import base64
return base64.b64encode(input.encode()).decode()
$$ LANGUAGE plpython3u IMMUTABLE;

-- Base64 decode
CREATE OR REPLACE FUNCTION substrate.b64_decode(input TEXT)
RETURNS TEXT AS $$
import base64
return base64.b64decode(input.encode()).decode()
$$ LANGUAGE plpython3u IMMUTABLE;

-- Hex encode
CREATE OR REPLACE FUNCTION substrate.hex_encode(input TEXT)
RETURNS TEXT AS $$
return input.encode().hex()
$$ LANGUAGE plpython3u IMMUTABLE;

-- Hex decode
CREATE OR REPLACE FUNCTION substrate.hex_decode(input TEXT)
RETURNS TEXT AS $$
return bytes.fromhex(input).decode()
$$ LANGUAGE plpython3u IMMUTABLE;

-- Generate UUIDv4
CREATE OR REPLACE FUNCTION substrate.uuid4()
RETURNS UUID AS $$
import uuid
return str(uuid.uuid4())
$$ LANGUAGE plpython3u;

-- Generate deterministic UUIDv5 (namespace + name)
CREATE OR REPLACE FUNCTION substrate.uuid5(ns TEXT, name TEXT)
RETURNS UUID AS $$
import uuid
ns_map = {
    'dns': uuid.NAMESPACE_DNS,
    'url': uuid.NAMESPACE_URL,
    'oid': uuid.NAMESPACE_OID,
    'x500': uuid.NAMESPACE_X500
}
ns_lower = ns.lower()
if ns_lower in ns_map:
    ns_uuid = ns_map[ns_lower]
else:
    ns_uuid = uuid.UUID(ns)
return str(uuid.uuid5(ns_uuid, name))
$$ LANGUAGE plpython3u IMMUTABLE;

-- Random bytes as hex
CREATE OR REPLACE FUNCTION substrate.random_hex(n_bytes INT DEFAULT 16)
RETURNS TEXT AS $$
import os
return os.urandom(n_bytes).hex()
$$ LANGUAGE plpython3u;

-- Simple hash-based consistent hashing (returns bucket 0..n-1)
CREATE OR REPLACE FUNCTION substrate.consistent_hash(key TEXT, n_buckets INT)
RETURNS INT AS $$
import hashlib
h = int(hashlib.md5(key.encode()).hexdigest(), 16)
return h % n_buckets
$$ LANGUAGE plpython3u IMMUTABLE;

-- Password/secret strength estimator (bits of entropy)
CREATE OR REPLACE FUNCTION substrate.password_entropy(pw TEXT)
RETURNS FLOAT8 AS $$
import math, re
charset = 0
if re.search(r'[a-z]', pw): charset += 26
if re.search(r'[A-Z]', pw): charset += 26
if re.search(r'[0-9]', pw): charset += 10
if re.search(r'[^a-zA-Z0-9]', pw): charset += 32
if charset == 0: return 0
return len(pw) * math.log2(charset)
$$ LANGUAGE plpython3u IMMUTABLE;
