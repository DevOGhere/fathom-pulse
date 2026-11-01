"""Incident detector: it must fire on real failures, stay quiet on normal wobble, and say only what the evidence supports."""
from pulse.api.incidents import Detector, Obs

A, B = "binance/btcusdt", "coinbase/btcusd"


def run(det, secs, *, ra=10, rb=16, age_a=0.1, age_b=0.1, lag=5, t0=0, induced=False):
    ev = []
    for i in range(secs):
        ev += det.update(Obs((t0 + i) * 1000, {A: {"rate": ra, "age_s": age_a}, B: {"rate": rb, "age_s": age_b}}, lag, induced=induced))
    return ev


def kinds(det):
    return sorted((i["kind"], i["feed"]) for i in det.snapshot())


def test_quiet_on_normal_wobble():
    d = Detector()
    for s in range(120):
        run(d, 1, ra=10 + (s % 3) - 1, rb=16 + (s % 5) - 2, lag=3 + s % 7, t0=s)
    assert d.snapshot() == []


def test_stall_opens_after_threshold_and_closes_on_recovery_with_upstream_cause():
    d = Detector()
    run(d, 60)
    ev = run(d, 6, ra=0, age_a=4.0, t0=60)                       # binance silent, coinbase fine, processor has no backlog
    assert [(k, i.kind) for k, i in ev] == [("open", "stall")]
    inc = d.snapshot()[0]
    assert inc["feed"] == A and "upstream of Fathom" in inc["cause"] and "only binance/btcusdt went quiet" in inc["cause"]
    run(d, 3, ra=10, age_a=0.2, t0=66)
    done = d.snapshot()[0]
    assert done["end_ms"] is not None and done["kind"] == "stall"


def test_both_feeds_silent_is_called_out_as_feed_wide():
    d = Detector()
    run(d, 60)
    run(d, 5, ra=0, rb=0, age_a=5, age_b=5, t0=60)
    assert {i["feed"] for i in d.snapshot()} == {A, B}
    assert all("both feeds went quiet together" in i["cause"] for i in d.snapshot())


def test_stall_with_backlog_does_not_blame_upstream():
    d = Detector()
    run(d, 60)
    run(d, 5, ra=0, age_a=5, lag=900, t0=60)
    c = next(i for i in d.snapshot() if i["kind"] == "stall")["cause"]
    assert "upstream" not in c and "not clear" in c


def test_rate_drop_is_detected_but_a_mild_dip_is_not():
    d = Detector()
    run(d, 90)
    run(d, 8, ra=7, t0=90)                                         # 30% dip: normal-ish, below the 50% ratio rule
    assert ("rate_drop", A) not in kinds(d)
    run(d, 8, ra=1, t0=98)                                         # collapse to 1/s while still alive
    assert ("rate_drop", A) in kinds(d)


def test_lag_spike_needs_three_seconds_in_a_row():
    d = Detector()
    run(d, 60)
    run(d, 2, lag=900, t0=60); run(d, 1, lag=5, t0=62)
    assert ("lag_spike", None) not in kinds(d)
    run(d, 4, lag=900, t0=63)
    assert ("lag_spike", None) in kinds(d)


def test_induced_overlap_is_labelled_induced_and_not_called_an_outage():
    d = Detector()
    run(d, 60)
    run(d, 6, ra=0, rb=0, age_a=4, age_b=4, induced=True, t0=60)
    incs = d.snapshot()
    assert incs and all(i["induced"] and i["cause"].startswith("Induced") for i in incs)


def test_recovery_after_a_stall_is_not_reported_as_a_second_rate_drop_incident():
    d = Detector()
    run(d, 60)
    run(d, 8, ra=0, age_a=5, t0=60)                  # stall opens
    run(d, 12, ra=10, age_a=0.2, t0=68)              # recovery: the fast window still holds zeros for ~5 s
    assert all(i["kind"] == "stall" for i in d.snapshot()), [(i["kind"], i["feed"]) for i in d.snapshot()]


def run_lat(det, secs, lat, t0, **kw):
    ev = []
    for i in range(secs):
        ev += det.update(Obs((t0 + i) * 1000, {A: {"rate": 10, "age_s": 0.1}, B: {"rate": 16, "age_s": 0.1}}, 5, lat_ms=lat, **kw))
    return ev


def test_slow_latency_rise_is_detected_while_records_keep_flowing():
    d = Detector()
    run_lat(d, 60, 20.0, 0)
    run_lat(d, 8, 90.0, 60)
    assert [i["kind"] for i in d.snapshot()] == ["latency_drift"]
    inc = d.snapshot()[0]
    assert "from a normal 20 ms" in inc["cause"] and inc["evidence"]["latency_normal_ms"] == 20.0
    assert inc["evidence"]["peak_ms"] >= 80.0                       # the opening text is the first crossing; the peak is kept
    run_lat(d, 10, 20.0, 68)
    assert d.snapshot()[0]["end_ms"] is not None


def test_small_or_proportionally_small_latency_changes_are_not_drift():
    d = Detector()
    run_lat(d, 60, 20.0, 0)
    run_lat(d, 10, 35.0, 60)                       # +15 ms: under the 20 ms floor
    assert d.snapshot() == []
    d2 = Detector()
    run_lat(d2, 60, 400.0, 0); run_lat(d2, 10, 430.0, 60)   # +30 ms but only 1.07x of a slow baseline
    assert d2.snapshot() == []


def test_drift_during_a_deliberate_test_is_labelled_induced():
    d = Detector()
    run_lat(d, 60, 20.0, 0)
    run_lat(d, 8, 100.0, 60, induced=True)
    assert d.snapshot()[0]["induced"] and d.snapshot()[0]["cause"].startswith("Induced")
