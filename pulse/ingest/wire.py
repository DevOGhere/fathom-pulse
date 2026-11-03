# COPIED from fathom/processor/wire.py @ fathom d2e63e4 (read-only upstream, S1: no cross-repo imports).
# Changes: import of marketdata_pb2 rewritten to the local copy; peek_index() added so callers can
# skip non-trade messages before ParseFromString. Decode logic is otherwise byte-for-byte upstream.
"""Confluent wire-format reader: routes RecordNameStrategy-multiplexed topics.

Phase 1 publishes multiple proto types per topic (decision A, MSG_032/033):
the value bytes are framed as

    [0x00 magic][4-byte BE schema id][msg-index array][protobuf payload]

where the msg-index array is a zigzag-varint count followed by zigzag-varint
indexes into the .proto's top-level message declarations (special case: the
array [0] is encoded as the single byte 0x00). Because all our subjects carry
the SAME schema text, the schema id cannot route, only the message index can.
So we parse the header ourselves and dispatch on the index. No per-message SR
lookup, no oneof envelope.

Index map = declaration order in proto/marketdata.proto (M4-frozen):
    0 Trade, 1 PriceLevel, 2 OrderBookSnapshot, 3 OrderBookDelta,
    4 DerivedFeatures, 5 OHLCVBar

Pinned empirically: tests/test_wire.py round-trips bytes produced by the real
confluent ProtobufSerializer (mock:// SR), not hand-rolled fixtures.
"""
from __future__ import annotations

import struct

from pulse.ingest import marketdata_pb2 as md

_MAGIC = 0
_INDEX_TO_TYPE = {
    0: md.Trade,
    1: md.PriceLevel,
    2: md.OrderBookSnapshot,
    3: md.OrderBookDelta,
    4: md.DerivedFeatures,
    5: md.OHLCVBar,
}


class WireError(ValueError):
    """Malformed Confluent-framed message."""


def _read_zigzag_varint(buf: bytes, pos: int) -> tuple[int, int]:
    """Decode one zigzag varint at `pos`; return (value, next_pos)."""
    shift = 0
    raw = 0
    while True:
        if pos >= len(buf):
            raise WireError("truncated varint in message-index header")
        b = buf[pos]
        pos += 1
        raw |= (b & 0x7F) << shift
        if not b & 0x80:
            break
        shift += 7
    return (raw >> 1) ^ -(raw & 1), pos


def peek_index(value: bytes) -> int:
    """Return the message-index (0 Trade, 2 Snapshot, 3 Delta, 4 Derived ...) WITHOUT parsing the payload."""
    if len(value) < 6 or value[0] != _MAGIC:
        raise WireError(f"bad magic/short frame (len={len(value)})")
    count, pos = _read_zigzag_varint(value, 5)
    if count == 0:
        return 0
    index, _ = _read_zigzag_varint(value, pos)
    return index


def decode(value: bytes):
    """Parse one Confluent-framed value -> deserialized proto message.

    Returns the concrete marketdata_pb2 message (Trade / OrderBookSnapshot /
    OrderBookDelta / ...). Raises WireError on malformed framing.
    """
    if len(value) < 6 or value[0] != _MAGIC:
        raise WireError(f"bad magic/short frame (len={len(value)})")
    # schema id (unused for routing, identical schema across subjects)
    (_schema_id,) = struct.unpack_from(">I", value, 1)

    count, pos = _read_zigzag_varint(value, 5)
    if count == 0:
        index = 0          # special case: array [0] encoded as single 0 byte
    else:
        index, pos = _read_zigzag_varint(value, pos)
        for _ in range(count - 1):     # nested paths, none in our schema
            _, pos = _read_zigzag_varint(value, pos)

    cls = _INDEX_TO_TYPE.get(index)
    if cls is None:
        raise WireError(f"unknown message index {index}")
    msg = cls()
    msg.ParseFromString(value[pos:])
    return msg
