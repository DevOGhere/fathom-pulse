"""Day 2 proof client:  python -m pulse.api.soak --seconds 600 [--slow-ms 500]

Connects one normal client (and optionally one deliberately slow client via the server's DEBUG ?throttle_ms hook) and
prints the P1-T3 gate evidence: frame rate, seq gaps, max inter-frame gap, lag flatness, pulse_age, negative-stage
count, drop counters. Exit non-zero if a hard check fails.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time

import websockets

URL = "ws://127.0.0.1:8765/stream"


async def client(seconds: float, qs: str, out: dict) -> None:
    seqs, arr, lag, age, neg, drops = [], [], [], [], 0, 0
    end = time.perf_counter() + seconds
    async with websockets.connect(URL + qs, max_size=None) as ws:
        while time.perf_counter() < end:
            try:
                f = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
            except asyncio.TimeoutError:
                out["timeouts"] = out.get("timeouts", 0) + 1
                continue
            arr.append(time.perf_counter()); seqs.append(f["seq"])
            if f["lag"]["total"] is not None:
                lag.append(f["lag"]["total"])
            if f["pulse_age_ms"] is not None:
                age.append(f["pulse_age_ms"])
            for s in f["series"].values():
                if s["window_1s"]:
                    neg += s["window_1s"]["neg_stage"]
            drops = f["client"]["drops"]
            out["last_frame"] = f
    out.update(seqs=seqs, arr=arr, lag=lag, age=age, neg=neg, drops=drops)


def pct(v, p):
    s = sorted(v)
    return s[min(len(s) - 1, int(p / 100 * len(s)))]


async def main(seconds: float, slow_ms: int) -> int:
    fast, slow = {}, {}
    jobs = [client(seconds, "", fast)]
    if slow_ms:
        jobs.append(client(seconds, f"?throttle_ms={slow_ms}", slow))
    await asyncio.gather(*jobs)
    ok = True
    seqs, arr = fast["seqs"], fast["arr"]
    span = arr[-1] - arr[0]
    gaps = [b - a for a, b in zip(seqs, seqs[1:]) if b - a != 1]
    ia = [b - a for a, b in zip(arr, arr[1:])]
    print(f"=== normal client, {seconds:.0f}s ===")
    print(f"frames={len(seqs)} measured_rate={(len(seqs) - 1) / span:.2f} Hz (target 10.0)  seq_gaps={len(gaps)}  recv_timeouts={fast.get('timeouts', 0)}")
    print(f"inter-frame gap ms: p50={pct(ia, 50) * 1000:.1f} p99={pct(ia, 99) * 1000:.1f} max={max(ia) * 1000:.1f}")
    ok &= abs((len(seqs) - 1) / span - 10.0) < 0.3 and not gaps
    lag = fast["lag"]
    if lag:
        third = max(1, len(lag) // 3)
        a, b = statistics.median(lag[:third]), statistics.median(lag[-third:])
        print(f"processor lag (msgs): min={min(lag)} median={statistics.median(lag):.0f} p99={pct(lag, 99)} max={max(lag)}  first-third median={a:.0f} last-third median={b:.0f}")
        ok &= (b <= a * 2 + 200)       # flat = no upward trend (loose: lag is naturally bursty)
    else:
        print("processor lag: NO SAMPLES"); ok = False
    age = fast["age"]
    print(f"pulse_age_ms: p50={pct(age, 50):.1f} p99={pct(age, 99):.1f} max={max(age):.1f}   negative stage durations={fast['neg']}")
    ok &= fast["neg"] == 0
    print(f"client drops (normal): {fast['drops']}")
    ok &= fast["drops"] == 0
    if slow_ms:
        s_seqs = slow["seqs"]
        print(f"\n=== slow client (server-side throttle {slow_ms} ms/frame) ===")
        print(f"frames received={len(s_seqs)} drops reported in-frame={slow['drops']}  (expect drops > 0; stream stays live)")
        ok &= slow["drops"] > 0 and len(s_seqs) > 0
        print("normal client unaffected by the slow one: seq_gaps =", len(gaps))
    print("\nlast frame sample:", json.dumps(fast["last_frame"]["series"], indent=1)[:900])
    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--slow-ms", type=int, default=0)
    sys.exit(asyncio.run(main(ap.parse_args().seconds, ap.parse_args().slow_ms)))
