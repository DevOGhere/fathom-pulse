"""Deterministic tests for the pure frame builder (P1-T3): frames are self-contained, windowed, honest about stages."""
from pulse.api.frames import build_frame, HIST_BINS
from pulse.ingest.rings import BIN_GRACE_NS, NS, Aggregator, DerivedRec

V, S = "binance", "btcusdt"
LAG = {"ok": True, "total": 5, "per_topic": {}, "age_ms": 100}
ING = {"queue_drops": 0, "late": 0}
IDENT = lambda m: m  # mono == wall in tests


def rec(wall_s, *, recv=0, derived=1_000_000, consume_mono=None, eff=None, mid=100.0, kafka_ms=0):
    w = int(wall_s * NS)
    return DerivedRec(V, S, derived, recv, w if consume_mono is None else consume_mono, w, kafka_ms,
                      mid, mid + 0.1, 0.2, 0.3, eff, mid - 0.5, mid + 0.5)


def frame(agg, now_s, seq=1):
    return build_frame(agg, seq, int(now_s * NS), IDENT, LAG, ING)


def test_window_is_the_trailing_second_only():
    a = Aggregator()
    for t in (10.0, 10.5, 11.2, 11.6, 11.9):
        a.add_derived(rec(t))
    w = frame(a, 12.0)["series"][f"{V}/{S}"]["window_1s"]
    assert w["n"] == 3                        # 11.2, 11.6, 11.9 (>= 11.0); 10.x is outside


def test_frame_is_self_contained_and_survives_missed_frames():
    a = Aggregator()
    for t in range(0, 30):
        a.add_derived(rec(100 + t, mid=100 + t))
        a.close_due(int((100 + t + 1) * NS) + BIN_GRACE_NS)
    f = frame(a, 130.2, seq=77)               # a client that saw nothing before this frame
    s = f["series"][f"{V}/{S}"]
    assert f["seq"] == 77 and f["v"] == 1
    assert len(s["bins"]["start_s"]) == 30 and s["bins"]["mid"][-1] == 129.0   # full history rides every frame
    assert len(s["bins"]["start_s"]) <= HIST_BINS


def test_sentinel_effective_spread_is_null_not_zero():
    a = Aggregator()
    a.add_derived(rec(10.1, eff=None))
    w = frame(a, 10.5)["series"][f"{V}/{S}"]["window_1s"]
    assert w["effective_spread"] is None
    a.add_derived(rec(10.3, eff=0.75))
    a.add_derived(rec(10.4, eff=None))
    assert frame(a, 10.5)["series"][f"{V}/{S}"]["window_1s"]["effective_spread"] == 0.75


def test_stage_durations_are_monotonic_differences_in_ms():
    a = Aggregator()
    a.add_derived(rec(10.1, recv=1_000_000, derived=4_000_000, consume_mono=9_000_000))
    w = frame(a, 10.5)["series"][f"{V}/{S}"]["window_1s"]
    assert w["stage_recv_to_derived_ms"]["p50"] == 3.0      # (4e6-1e6)/1e6
    assert w["stage_derived_to_consume_ms"]["p50"] == 5.0   # (9e6-4e6)/1e6
    assert w["neg_stage"] == 0


def test_negative_stage_is_counted_not_clamped():
    a = Aggregator()
    a.add_derived(rec(10.1, recv=9_000_000, derived=4_000_000, consume_mono=3_000_000))   # both stages negative
    w = frame(a, 10.5)["series"][f"{V}/{S}"]["window_1s"]
    assert w["neg_stage"] == 2 and w["stage_recv_to_derived_ms"]["p50"] == -5.0


def test_ages_use_the_right_clocks():
    a = Aggregator()
    a.add_derived(rec(10.0, derived=int(9.9 * NS), kafka_ms=9_950))
    w = frame(a, 10.2)["series"][f"{V}/{S}"]["window_1s"]
    assert abs(w["derived_age_ms"] - 300.0) < 0.01          # now(10.2) - bridged derived_at(9.9)
    assert abs(w["pulse_consume_age_ms"] - 200.0) < 0.01    # now - consume wall (10.0)
    assert w["kafka_age_ms"] == 10_200 - 9_950


def test_empty_state_has_honest_nulls():
    f = frame(Aggregator(), 10.0)
    assert f["series"] == {} and f["pulse_age_ms"] is None


def test_trade_tail_and_aggressor_side():
    from pulse.ingest.rings import TradeRec
    a = Aggregator()
    a.add_derived(rec(10.0))
    # is_buyer_maker=True => buyer was the maker => aggressor is the SELLER (same mapping on Binance and Coinbase upstream)
    a.add_trade(TradeRec(V, S, 100.0, 0.5, 0, int(10.1 * NS), 0, True))
    a.add_trade(TradeRec(V, S, 101.0, 0.2, 0, int(10.2 * NS), 0, False))
    t = frame(a, 10.5)["series"][f"{V}/{S}"]["trades"]
    assert [x["aggressor"] for x in t] == ["sell", "buy"] and t[0]["px"] == 100.0 and t[1]["qty"] == 0.2


def test_top_of_book_quantities_ride_the_frame():
    a = Aggregator()
    r = rec(10.0)._replace(best_bid_qty=1.5, best_ask_qty=2.5)
    a.add_derived(r)
    w = frame(a, 10.2)["series"][f"{V}/{S}"]["window_1s"]
    assert (w["best_bid"], w["best_ask"], w["best_bid_qty"], w["best_ask_qty"]) == (99.5, 100.5, 1.5, 2.5)


def test_slo_excludes_scheduled_tick_wait_but_reports_it():
    a = Aggregator()
    a.add_derived(rec(10.1, recv=1_000_000, derived=4_000_000, consume_mono=9_000_000))
    ready = a._keys[(V, S)].pending[0][3]
    a.record_stage3(ready + 79_000_000)                     # frame built 79 ms after the record was ready
    slo = frame(a, 10.5)["slo"]
    e = a._keys[(V, S)].stages[-1]
    assert slo["tick_wait_p50_ms"] == 79.0
    assert slo["p99_ms"] == round(3.0 + 5.0 + e[3], 3)      # s1 + s2 + s3 (consume -> ready) = pipeline only
    assert slo["end_to_frame_p99_ms"] > slo["p99_ms"] + 78  # the full figure stays visible
