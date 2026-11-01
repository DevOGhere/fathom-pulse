"""pulse.fv.1 contract test: the shape is frozen (additive-only), closed bins only, honest nulls, latest == window shape."""
from pulse.api import features as fv
from pulse.ingest.rings import BIN_GRACE_NS, NS, Aggregator, DerivedRec

V, S = "binance", "btcusdt"

# GOLDEN: the fv.1 field list. Changing or removing an entry is a breaking change (fv.2). Appending is allowed: edit deliberately.
GOLDEN = ["schema_version", "venue", "symbol", "ts_ms", "bin", "mid", "microprice", "spread_abs", "obi_top1", "obi_top5",
          "effective_spread", "realized_vol_60s", "quote_rate_1s", "trade_count_1s", "avg_trade_size_60s", "empty", "induced", "provenance"]


def rec(wall_s, mid=100.0, eff=None):
    w = int(wall_s * NS)
    return DerivedRec(V, S, 4_000_000, 1_000_000, 9_000_000, w, 0, mid, mid + 0.1, 0.2, 0.3, eff, mid - 0.5, mid + 0.5)


def closed_agg():
    a = Aggregator()
    a.add_derived(rec(10.1)); a.add_derived(rec(10.6, eff=0.4)); a.add_derived(rec(12.2, mid=101.0))   # second 11 is empty
    a.close_due(int(13 * NS + BIN_GRACE_NS + 1))
    return a


def test_field_list_is_frozen_to_golden():
    assert [f for f, _ in fv.FIELDS] == GOLDEN
    v = fv.latest(closed_agg().bins(V, S), 13_500)
    assert list(v) == GOLDEN
    assert set(v["provenance"]) == set(fv.PROVENANCE_FIELDS)


def test_every_field_has_the_declared_type():
    for v in fv.window(closed_agg().bins(V, S), 13_500):
        for name, types in fv.FIELDS:
            assert isinstance(v[name], types), f"{name}={v[name]!r}"


def test_only_closed_bins_are_served_and_latest_is_the_newest():
    a = Aggregator()
    a.add_derived(rec(10.1))
    assert fv.latest(a.bins(V, S), 10_500) is None          # nothing closed yet: no partial bin leaks
    a = closed_agg()
    assert fv.latest(a.bins(V, S), 13_500)["ts_ms"] == a.bins(V, S)[-1].close_ts_ms


def test_empty_second_is_honest_null_not_zero():
    vs = fv.window(closed_agg().bins(V, S), 13_500)
    empty = [v for v in vs if v["empty"]]
    assert empty and all(v["mid"] is None and v["quote_rate_1s"] == 0 and v["effective_spread"] is None for v in empty)


def test_sentinel_effective_spread_is_null():
    vs = fv.window(closed_agg().bins(V, S), 13_500)
    first = next(v for v in vs if not v["empty"])
    assert first["effective_spread"] == 0.4 and all(v["effective_spread"] != 0.0 for v in vs)


def test_latest_and_window_have_identical_shape():
    bins = closed_agg().bins(V, S)
    assert list(fv.latest(bins, 13_500)) == list(fv.window(bins, 13_500)[-1])


def test_window_range_is_exclusive_from_inclusive_to_and_age_is_measured():
    bins = closed_agg().bins(V, S)
    closes = [b.close_ts_ms for b in bins]
    got = fv.window(bins, closes[-1] + 250, from_ms=closes[0], to_ms=closes[-1])
    assert [v["ts_ms"] for v in got] == closes[1:]
    assert got[-1]["provenance"]["pulse_age_ms"] == 250
