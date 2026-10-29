"""Day 1 live proof runner:  python -m pulse.ingest --seconds 60

Consumes the live topics, aggregates into closed 1s bins, and prints the evidence the P1 gates ask for:
per-topic rates, one fully decoded DerivedFeatures message, bins-per-key cadence check, one sample bin as JSON,
and process RSS. Exit code is non-zero if the cadence or memory checks fail.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

import psutil
from google.protobuf.json_format import MessageToJson

from pulse.ingest.consumer import IngestConsumer, TOPICS
from pulse.ingest.rings import NS, Aggregator, DerivedRec, TradeRec

RSS_LIMIT_MB = 300


async def run(seconds: float) -> int:
    loop = asyncio.get_running_loop()
    q: asyncio.Queue = asyncio.Queue(maxsize=1000)
    agg, ic = Aggregator(), IngestConsumer(loop, q)
    closed: dict[tuple[str, str], list] = {}
    stop = time.monotonic() + seconds
    proc = psutil.Process()
    peak_rss = 0.0

    async def pump() -> None:
        while True:
            for r in await q.get():
                if isinstance(r, DerivedRec):
                    agg.add_derived(r)
                elif isinstance(r, TradeRec):
                    agg.add_trade(r)

    async def closer() -> None:
        while True:
            for b in agg.close_due(time.time_ns()):
                closed.setdefault((b.venue, b.symbol), []).append(b)
            agg.assert_caps()
            await asyncio.sleep(0.05)

    tasks = [asyncio.create_task(pump()), asyncio.create_task(closer())]
    ic.start()
    t0 = time.monotonic()
    last = 0.0
    while time.monotonic() < stop:
        await asyncio.sleep(0.5)
        peak_rss = max(peak_rss, proc.memory_info().rss / 1e6)
        if time.monotonic() - t0 - last >= 10:
            last = time.monotonic() - t0
            print(f"[{last:5.0f}s] msgs={sum(ic.counts.values())} bins={sum(len(v) for v in closed.values())} "
                  f"late={agg.late} qdrops={ic.queue_drops} rss={peak_rss:.0f}MB", flush=True)
    ic.stop()
    for t in tasks:
        t.cancel()
    elapsed = time.monotonic() - t0
    for b in agg.close_due(time.time_ns()):
        closed.setdefault((b.venue, b.symbol), []).append(b)

    ok = True
    print("\n=== per-topic rates ===")
    for t in TOPICS:
        print(f"{t:36s} seen={ic.counts[t]:6d} ({ic.counts[t] / elapsed:6.1f}/s)  decoded={ic.decoded[t]:6d} ({ic.decoded[t] / elapsed:6.1f}/s)")
    print(f"decode/consume errors: {dict(ic.errors) or 'none'}   queue drops: {ic.queue_drops}   late-dropped: {agg.late}")

    print("\n=== one decoded DerivedFeatures message (proves the Confluent frame is handled) ===")
    print(MessageToJson(ic.sample_derived, preserving_proto_field_name=True) if ic.sample_derived else "NONE RECEIVED")
    if ic.sample_derived is None:
        ok = False

    print("\n=== bin cadence: exactly one bin per second per symbol ===")
    for key, bins in sorted(closed.items()):
        secs = [b.start_s for b in bins]
        consecutive = all(b - a == 1 for a, b in zip(secs, secs[1:]))
        nonempty = sum(1 for b in bins if not b.empty)
        span = secs[-1] - secs[0] + 1 if secs else 0
        verdict = "OK" if consecutive and len(set(secs)) == len(secs) and span == len(secs) else "FAIL"
        ok &= verdict == "OK"
        print(f"{key[0]}/{key[1]:8s} bins={len(bins):4d} span={span:4d}s consecutive={consecutive} "
              f"non-empty={nonempty} empty={len(bins) - nonempty}  -> {verdict}")

    print("\n=== sample bins (sentinel handling visible: effective_spread null) ===")
    shown = 0
    for key, bins in sorted(closed.items()):
        for b in bins[len(bins) // 2:]:
            if not b.empty and (b.effective_spread is None or shown == 0):
                print(json.dumps(b.to_dict(), indent=1))
                shown += 1
                break
        if shown >= 2:
            break

    print(f"\n=== memory === peak RSS {peak_rss:.0f} MB (limit {RSS_LIMIT_MB} MB)")
    ok &= peak_rss < RSS_LIMIT_MB
    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=60)
    sys.exit(asyncio.run(run(ap.parse_args().seconds)))
