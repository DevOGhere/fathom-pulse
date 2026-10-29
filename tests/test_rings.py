"""Deterministic tests for the closed-bin aggregator and the wire framing (P1-T1/T2)."""
import math

import pytest

from pulse.ingest import marketdata_pb2 as md
from pulse.ingest import wire
from pulse.ingest.rings import BIN_GRACE_NS, NS, Aggregator, DerivedRec, TradeRec

V, S = "binance", "btcusdt"


def drec(sec, frac=0.1, mid=100.0, eff=None, bid=99.5, ask=100.5, venue=V, symbol=S):
    w = int((sec + frac) * NS)
    return DerivedRec(venue, symbol, 0, 0, w, w, 0, mid, mid + 0.1, 0.2, 0.3, eff, bid, ask)


def trec(sec, qty=0.5, frac=0.2):
    w = int((sec + frac) * NS)
    return TradeRec(V, S, 100.0, qty, w, w, 0)


def test_bin_stays_open_until_deadline_plus_grace():
    a = Aggregator()
    a.add_derived(drec(10))
    assert a.close_due(11 * NS) == []                         # second over, grace not yet
    assert a.close_due(11 * NS + BIN_GRACE_NS - 1) == []      # one ns before the deadline
    out = a.close_due(11 * NS + BIN_GRACE_NS)
    assert len(out) == 1 and out[0].start_s == 10 and out[0].close_ts_ms == 11_000


def test_empty_seconds_emit_explicit_empty_bins_with_exact_cadence():
    a = Aggregator()
    a.add_derived(drec(10))
    out = a.close_due(14 * NS + 500_000_000)                  # seconds 10..13 are closeable
    assert [b.start_s for b in out] == [10, 11, 12, 13]
    assert [b.empty for b in out] == [False, True, True, True]
    e = out[1]
    assert e.n_derived == 0 and e.mid is None and e.obi_top1 is None and e.effective_spread is None
    # calling again with a later clock continues the sequence with no gap and no repeat
    out2 = a.close_due(16 * NS + 500_000_000)
    assert [b.start_s for b in out2] == [14, 15]


def test_every_second_exactly_once_over_a_long_run():
    a = Aggregator()
    seen = []
    for t in range(0, 600):                                   # 60 s of 10Hz ticks
        sec = 100 + t // 10
        a.add_derived(drec(sec, frac=(t % 10) / 10))
        seen += [b.start_s for b in a.close_due(int((100 + t / 10) * NS) + 1)]
    seen += [b.start_s for b in a.close_due(200 * NS)]
    assert seen == list(range(100, 160)) + list(range(160, 199))   # consecutive, no dupes, no gaps


def test_late_message_is_counted_and_never_rewrites_a_closed_bin():
    a = Aggregator()
    a.add_derived(drec(10))
    (b,) = a.close_due(11 * NS + BIN_GRACE_NS)
    assert a.add_derived(drec(10, frac=0.9)) is False
    assert a.late == 1
    assert a.bins(V, S)[0].n_derived == b.n_derived == 1


def test_sentinel_is_never_averaged_or_zero_filled():
    a = Aggregator()
    a.add_derived(drec(10, frac=0.1, eff=0.8))
    a.add_derived(drec(10, frac=0.5, eff=None))               # sentinel -> None: must not clobber 0.8
    a.add_derived(drec(11, frac=0.1, eff=None))               # a second with ONLY sentinels -> null, not 0.0
    out = a.close_due(12 * NS + BIN_GRACE_NS)
    assert out[0].effective_spread == 0.8
    assert out[1].effective_spread is None


def test_bin_kpis_use_last_value_and_count_quote_rate():
    a = Aggregator()
    for i, m in enumerate([100.0, 101.0, 102.0]):
        a.add_derived(drec(10, frac=0.1 + i * 0.1, mid=m, bid=m - 0.5, ask=m + 0.5))
    (b,) = a.close_due(11 * NS + BIN_GRACE_NS)
    assert b.n_derived == 3 and b.mid == 102.0 and b.spread_abs == pytest.approx(1.0)


def test_realized_vol_matches_hand_calculation_and_skips_empty_bins():
    a = Aggregator()
    mids = [100.0, 101.0, 100.0]
    for i, m in enumerate(mids):
        a.add_derived(drec(10 + i, mid=m))
    a.add_derived(drec(14, mid=102.0))                         # second 13 stays empty
    bins = a.close_due(15 * NS + BIN_GRACE_NS)
    assert bins[0].realized_vol_60s is None and bins[1].realized_vol_60s is None   # <2 returns
    rets = [math.log(101 / 100), math.log(100 / 101)]
    assert bins[2].realized_vol_60s == pytest.approx(math.sqrt(sum(r * r for r in rets)))
    rets.append(math.log(102 / 100))                           # empty bin 13 contributes no return
    assert bins[4].realized_vol_60s == pytest.approx(math.sqrt(sum(r * r for r in rets)))


def test_trade_stats():
    a = Aggregator()
    a.add_derived(drec(10))
    a.add_trade(trec(10, qty=1.0))
    a.add_trade(trec(10, qty=3.0, frac=0.6))
    (b,) = a.close_due(11 * NS + BIN_GRACE_NS)
    assert b.trade_count_1s == 2 and b.avg_trade_size_60s == pytest.approx(2.0)


def test_no_trades_gives_null_average_not_zero():
    a = Aggregator()
    a.add_derived(drec(10))
    (b,) = a.close_due(11 * NS + BIN_GRACE_NS)
    assert b.trade_count_1s == 0 and b.avg_trade_size_60s is None


def test_ring_caps_hold_under_load():
    a = Aggregator(bin_cap=30, raw_cap=100)
    for t in range(2000):
        a.add_derived(drec(t // 10, frac=(t % 10) / 10))
        a.close_due(int((t / 10 + 0.5) * NS) + BIN_GRACE_NS)
        a.assert_caps()
    assert len(a.bins(V, S)) == 30


def test_keys_are_independent():
    a = Aggregator()
    a.add_derived(drec(10, venue="binance"))
    a.add_derived(drec(12, venue="coinbase", symbol="btcusd"))
    out = a.close_due(14 * NS + BIN_GRACE_NS)
    by = {}
    for b in out:
        by.setdefault((b.venue, b.symbol), []).append(b.start_s)
    assert by[("binance", "btcusdt")] == [10, 11, 12, 13] and by[("coinbase", "btcusd")] == [12, 13]


# ---- wire framing ----
def frame(index_bytes: bytes, payload: bytes) -> bytes:
    return b"\x00" + (1).to_bytes(4, "big") + index_bytes + payload


def test_peek_and_decode_trade_special_case_index_zero():
    t = md.Trade(venue="binance", symbol="btcusdt", price=1.5, quantity=0.25)
    v = frame(b"\x00", t.SerializeToString())                 # [0] encoded as the single byte 0x00
    assert wire.peek_index(v) == 0
    assert wire.decode(v) == t


def test_peek_and_decode_derived_with_explicit_index_array():
    d = md.DerivedFeatures(venue="coinbase", symbol="btcusd", mid_price=10.0, effective_spread=0.0)
    v = frame(bytes([2, 8]), d.SerializeToString())           # count=1 (zigzag 2), index=4 (zigzag 8)
    assert wire.peek_index(v) == 4
    assert wire.decode(v) == d


def test_malformed_frames_raise():
    with pytest.raises(wire.WireError):
        wire.peek_index(b"\x01\x00\x00\x00\x01\x00")
    with pytest.raises(wire.WireError):
        wire.peek_index(b"\x00\x00")
