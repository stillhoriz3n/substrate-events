-- ============================================================
-- SUBSTRATE LIBRARY: lib.wire
-- Serialization, framing, checksums, byte-level primitives
-- ============================================================

-- ===== BYTE ORDER =====

-- Big-endian encode unsigned 16-bit
CREATE OR REPLACE FUNCTION substrate.pack_u16_be(val INT)
RETURNS BYTEA AS $$
return bytes([(val >> 8) & 0xFF, val & 0xFF])
$$ LANGUAGE plpython3u IMMUTABLE;

-- Big-endian decode unsigned 16-bit
CREATE OR REPLACE FUNCTION substrate.unpack_u16_be(raw BYTEA)
RETURNS INT AS $$
b = bytes(raw)
return (b[0] << 8) | b[1]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Big-endian encode unsigned 32-bit
CREATE OR REPLACE FUNCTION substrate.pack_u32_be(val BIGINT)
RETURNS BYTEA AS $$
return bytes([(val >> 24) & 0xFF, (val >> 16) & 0xFF, (val >> 8) & 0xFF, val & 0xFF])
$$ LANGUAGE plpython3u IMMUTABLE;

-- Big-endian decode unsigned 32-bit
CREATE OR REPLACE FUNCTION substrate.unpack_u32_be(raw BYTEA)
RETURNS BIGINT AS $$
b = bytes(raw)
return (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3]
$$ LANGUAGE plpython3u IMMUTABLE;

-- Big-endian encode unsigned 64-bit
CREATE OR REPLACE FUNCTION substrate.pack_u64_be(val BIGINT)
RETURNS BYTEA AS $$
return val.to_bytes(8, 'big')
$$ LANGUAGE plpython3u IMMUTABLE;

-- Little-endian encode unsigned 32-bit
CREATE OR REPLACE FUNCTION substrate.pack_u32_le(val BIGINT)
RETURNS BYTEA AS $$
return bytes([val & 0xFF, (val >> 8) & 0xFF, (val >> 16) & 0xFF, (val >> 24) & 0xFF])
$$ LANGUAGE plpython3u IMMUTABLE;

-- Little-endian decode unsigned 32-bit
CREATE OR REPLACE FUNCTION substrate.unpack_u32_le(raw BYTEA)
RETURNS BIGINT AS $$
b = bytes(raw)
return b[0] | (b[1] << 8) | (b[2] << 16) | (b[3] << 24)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== CHECKSUMS =====

-- Internet checksum (RFC 1071) — used in IP, TCP, UDP headers
CREATE OR REPLACE FUNCTION substrate.inet_checksum(data BYTEA)
RETURNS INT AS $$
b = bytes(data)
if len(b) % 2: b += b'\x00'
total = 0
for i in range(0, len(b), 2):
    total += (b[i] << 8) | b[i+1]
while total >> 16:
    total = (total & 0xFFFF) + (total >> 16)
return (~total) & 0xFFFF
$$ LANGUAGE plpython3u IMMUTABLE;

-- Adler-32 checksum
CREATE OR REPLACE FUNCTION substrate.adler32(data BYTEA)
RETURNS BIGINT AS $$
import zlib
return zlib.adler32(bytes(data)) & 0xFFFFFFFF
$$ LANGUAGE plpython3u IMMUTABLE;

-- Fletcher-16 checksum
CREATE OR REPLACE FUNCTION substrate.fletcher16(data BYTEA)
RETURNS INT AS $$
sum1 = sum2 = 0
for b in bytes(data):
    sum1 = (sum1 + b) % 255
    sum2 = (sum2 + sum1) % 255
return (sum2 << 8) | sum1
$$ LANGUAGE plpython3u IMMUTABLE;

-- XOR checksum (simple parity)
CREATE OR REPLACE FUNCTION substrate.xor_checksum(data BYTEA)
RETURNS INT AS $$
result = 0
for b in bytes(data):
    result ^= b
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- Luhn check digit (credit card, IMEI)
CREATE OR REPLACE FUNCTION substrate.luhn_check(digits TEXT)
RETURNS BOOLEAN AS $$
d = [int(c) for c in digits if c.isdigit()]
d.reverse()
total = 0
for i, v in enumerate(d):
    if i % 2 == 1:
        v *= 2
        if v > 9: v -= 9
    total += v
return total % 10 == 0
$$ LANGUAGE plpython3u IMMUTABLE;

-- Luhn generate check digit
CREATE OR REPLACE FUNCTION substrate.luhn_generate(digits TEXT)
RETURNS TEXT AS $$
for check in range(10):
    candidate = digits + str(check)
    d = [int(c) for c in candidate if c.isdigit()]
    d.reverse()
    total = 0
    for i, v in enumerate(d):
        if i % 2 == 1:
            v *= 2
            if v > 9: v -= 9
        total += v
    if total % 10 == 0:
        return candidate
return None
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== FRAMING =====

-- Length-prefix frame: 4-byte big-endian length + data
CREATE OR REPLACE FUNCTION substrate.frame_length_prefix(data BYTEA)
RETURNS BYTEA AS $$
length = len(bytes(data))
header = length.to_bytes(4, 'big')
return header + bytes(data)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Decode length-prefix frame: extract length from first 4 bytes
CREATE OR REPLACE FUNCTION substrate.unframe_length(header_bytes BYTEA)
RETURNS INT AS $$
b = bytes(header_bytes)
return int.from_bytes(b[:4], 'big')
$$ LANGUAGE plpython3u IMMUTABLE;

-- COBS encode (Consistent Overhead Byte Stuffing) — zero-free framing
CREATE OR REPLACE FUNCTION substrate.cobs_encode(data BYTEA)
RETURNS BYTEA AS $$
b = bytes(data)
output = bytearray()
idx = 0
while idx < len(b):
    block_start = len(output)
    output.append(0)  # placeholder
    block_len = 1
    while idx < len(b) and b[idx] != 0 and block_len < 255:
        output.append(b[idx])
        idx += 1
        block_len += 1
    output[block_start] = block_len
    if idx < len(b) and b[idx] == 0:
        idx += 1
return bytes(output) + b'\x00'  # trailing delimiter
$$ LANGUAGE plpython3u IMMUTABLE;

-- COBS decode
CREATE OR REPLACE FUNCTION substrate.cobs_decode(data BYTEA)
RETURNS BYTEA AS $$
b = bytes(data)
if b and b[-1] == 0: b = b[:-1]  # strip delimiter
output = bytearray()
idx = 0
while idx < len(b):
    code = b[idx]
    idx += 1
    for _ in range(code - 1):
        if idx < len(b):
            output.append(b[idx])
            idx += 1
    if code < 255 and idx < len(b):
        output.append(0)
return bytes(output)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== TLV (Type-Length-Value) =====

-- TLV encode single entry: 2-byte type + 2-byte length + value
CREATE OR REPLACE FUNCTION substrate.tlv_encode(type_id INT, val BYTEA)
RETURNS BYTEA AS $$
t = type_id.to_bytes(2, 'big')
l = len(bytes(val)).to_bytes(2, 'big')
return t + l + bytes(val)
$$ LANGUAGE plpython3u IMMUTABLE;

-- TLV decode first entry from buffer: returns {type, length, value_hex, consumed}
CREATE OR REPLACE FUNCTION substrate.tlv_decode(buf BYTEA)
RETURNS JSONB AS $$
import json
b = bytes(buf)
if len(b) < 4: return json.dumps({'error': 'too short'})
type_id = (b[0] << 8) | b[1]
length = (b[2] << 8) | b[3]
if len(b) < 4 + length: return json.dumps({'error': 'truncated'})
value = b[4:4+length]
return json.dumps({'type': type_id, 'length': length, 'value_hex': value.hex(), 'consumed': 4 + length})
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== VARINT / COMPACT ENCODING =====

-- LEB128 encode (unsigned, used in DWARF, WebAssembly, protobuf)
CREATE OR REPLACE FUNCTION substrate.leb128_encode(input_val BIGINT)
RETURNS BYTEA AS $$
v = input_val
result = bytearray()
while True:
    byte = v & 0x7F
    v >>= 7
    if v != 0:
        byte |= 0x80
    result.append(byte)
    if v == 0:
        break
return bytes(result)
$$ LANGUAGE plpython3u IMMUTABLE;

-- LEB128 decode (unsigned)
CREATE OR REPLACE FUNCTION substrate.leb128_decode(data BYTEA)
RETURNS BIGINT AS $$
b = bytes(data)
result = 0
shift = 0
for byte in b:
    result |= (byte & 0x7F) << shift
    if not (byte & 0x80):
        break
    shift += 7
return result
$$ LANGUAGE plpython3u IMMUTABLE;

-- Protobuf zigzag encode (maps signed to unsigned for efficient varint)
CREATE OR REPLACE FUNCTION substrate.zigzag_encode(val BIGINT)
RETURNS BIGINT AS $$ SELECT CASE WHEN val >= 0 THEN val * 2 ELSE (-val) * 2 - 1 END $$ LANGUAGE sql IMMUTABLE;

-- Protobuf zigzag decode
CREATE OR REPLACE FUNCTION substrate.zigzag_decode(val BIGINT)
RETURNS BIGINT AS $$ SELECT CASE WHEN val % 2 = 0 THEN val / 2 ELSE -(val + 1) / 2 END $$ LANGUAGE sql IMMUTABLE;

-- ===== ESCAPE SEQUENCES =====

-- Byte-stuff escape (PPP-style): escape_byte and flag_byte
CREATE OR REPLACE FUNCTION substrate.byte_stuff(data BYTEA, flag INT DEFAULT 126, esc INT DEFAULT 125)
RETURNS BYTEA AS $$
b = bytes(data)
output = bytearray([flag])
for byte in b:
    if byte == flag or byte == esc:
        output.append(esc)
        output.append(byte ^ 0x20)
    else:
        output.append(byte)
output.append(flag)
return bytes(output)
$$ LANGUAGE plpython3u IMMUTABLE;

-- Byte-stuff decode
CREATE OR REPLACE FUNCTION substrate.byte_unstuff(data BYTEA, flag INT DEFAULT 126, esc INT DEFAULT 125)
RETURNS BYTEA AS $$
b = bytes(data)
if b and b[0] == flag: b = b[1:]
if b and b[-1] == flag: b = b[:-1]
output = bytearray()
i = 0
while i < len(b):
    if b[i] == esc and i + 1 < len(b):
        output.append(b[i+1] ^ 0x20)
        i += 2
    else:
        output.append(b[i])
        i += 1
return bytes(output)
$$ LANGUAGE plpython3u IMMUTABLE;

-- ===== SUBSTRATE NATIVE FRAMING =====

-- Substrate blob envelope: header for mesh transport
-- Format: magic(4) + version(1) + type(1) + flags(2) + blob_unid(16) + length(4) + checksum(4) = 32 bytes
CREATE OR REPLACE FUNCTION substrate.blob_envelope(
    blob_unid UUID, payload_len INT,
    msg_type INT DEFAULT 1, flags INT DEFAULT 0
)
RETURNS JSONB AS $$
import json, hashlib
magic = 'SUB1'
header_size = 32
return json.dumps({
    'magic': magic,
    'version': 1,
    'type': msg_type,
    'type_name': {1:'data',2:'manifest',3:'signal',4:'heartbeat',5:'ack',6:'nack'}.get(msg_type,'unknown'),
    'flags': flags,
    'blob_unid': str(blob_unid),
    'payload_length': payload_len,
    'total_size': header_size + payload_len,
    'header_size': header_size
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Substrate message types
CREATE OR REPLACE FUNCTION substrate.msg_types()
RETURNS JSONB AS $$
import json
return json.dumps({
    1: 'data',       2: 'manifest',    3: 'signal',
    4: 'heartbeat',  5: 'ack',         6: 'nack',
    7: 'subscribe',  8: 'unsubscribe', 9: 'query',
    10: 'response',  11: 'error',      12: 'ping',
    13: 'pong',      14: 'announce',   15: 'retire',
    16: 'force',     17: 'radiate',    18: 'sync_request',
    19: 'sync_response', 20: 'capability'
})
$$ LANGUAGE plpython3u IMMUTABLE;

-- Substrate peer announcement builder
CREATE OR REPLACE FUNCTION substrate.peer_announce(
    peer_id TEXT, endpoint TEXT, capabilities TEXT[],
    blob_count INT DEFAULT 0, version TEXT DEFAULT 'genesis-v1'
)
RETURNS JSONB AS $$
import json, time
return json.dumps({
    'type': 'announce',
    'peer_id': peer_id,
    'endpoint': endpoint,
    'capabilities': capabilities,
    'blob_count': blob_count,
    'substrate_version': version,
    'timestamp': time.time()
})
$$ LANGUAGE plpython3u;
