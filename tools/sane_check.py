"""Cross-check Pulse against Fathom's own Prometheus metrics (read-only):  .venv/bin/python tools/sane_check.py

1. RATE: Pulse's derived quote rate (closed 1 s bins, last 60 s) vs Prometheus rate(fathom_tick_to_store_latency_ns_count{table="derived_features"}[60s]).
   Different counters of the same stream; they should agree within a few percent.
2. ORDER: Pulse sees a record BEFORE it is stored, so Pulse's recv->consume latency must not exceed Fathom's tick->store latency
   (mean from the histogram). If it did, one of the two clocks/definitions would be wrong.
Prometheus is read at localhost:9090 (compose publishes it); nothing is written anywhere.
"""
import json, sys, urllib.parse, urllib.request

PULSE, PROM = "http://127.0.0.1:8765", "http://127.0.0.1:9090"


def get(url):
    return json.load(urllib.request.urlopen(url, timeout=5))


def prom(q):
    r = get(f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": q}))["data"]["result"]
    return {x["metric"].get("venue"): float(x["value"][1]) for x in r}


bad = 0
rate = prom('rate(fathom_tick_to_store_latency_ns_count{table="derived_features"}[60s])')
mean_store = {v: s / c for (v, s), c in zip(prom('increase(fathom_tick_to_store_latency_ns_sum{table="derived_features"}[60s])').items(), prom('increase(fathom_tick_to_store_latency_ns_count{table="derived_features"}[60s])').values()) if c}
print(f"{'venue':<9}{'pulse /s':>10}{'prom /s':>10}{'diff':>8}   {'pulse recv->consume mean':>26}{'fathom tick->store mean':>26}")
for venue, symbol in (("binance", "btcusdt"), ("coinbase", "btcusd")):
    bins = get(f"{PULSE}/features/window?venue={venue}&symbol={symbol}")["vectors"][-60:]
    live = [b for b in bins if not b["induced"]]
    pr = sum(b["quote_rate_1s"] for b in bins) / max(1, len(bins))
    pm = rate.get(venue)
    diff = None if not pm else (pr - pm) / pm
    lag = [b["provenance"]["derived_lag_ms_p50"] for b in live if b["provenance"]["derived_lag_ms_p50"] is not None]
    pulse_ms = sum(lag) / len(lag) if lag else None
    store_ms = mean_store.get(venue)
    store_ms = None if store_ms is None else store_ms / 1e6
    ok_rate = diff is not None and abs(diff) < 0.10
    ok_order = pulse_ms is not None and store_ms is not None and pulse_ms <= store_ms
    bad += (not ok_rate) + (not ok_order)
    f = lambda x, n=1: "n/a" if x is None else f"{x:.{n}f}"
    print(f"{venue:<9}{f(pr):>10}{f(pm):>10}{('n/a' if diff is None else f'{diff * 100:+.1f}%'):>8}   {f(pulse_ms) + ' ms (p50 of bin p50s)':>26}{f(store_ms) + ' ms':>26}   rate {'OK' if ok_rate else 'CHECK'}  order {'OK' if ok_order else 'CHECK'}")
print("\nNote: fathom's tick->store includes the ClickHouse/Timescale write; the sealed P99 63.4 ms needs the benchmark's flags, this processor runs defaults.")
sys.exit(1 if bad else 0)
