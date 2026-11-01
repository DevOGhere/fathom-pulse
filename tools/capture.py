"""Record live Pulse frames to NDJSON for browser replay:  .venv/bin/python tools/capture.py <seconds> <name> [--title "..."]

Writes pulse/web/captures/<name>.ndjson. Line 1 = {"capture": {...header...}}, then one frame per line exactly as the server
sent it, minus the per-client `client` block (replay supplies its own). CRYPTO ONLY (Binance + Coinbase live feeds): this repo's
public-data policy keeps CME/Databento-derived data off public hosting. The header names the sources so that stays checkable.
"""
import argparse, asyncio, json, time
from pathlib import Path
import websockets

ap = argparse.ArgumentParser(); ap.add_argument("seconds", type=float); ap.add_argument("name"); ap.add_argument("--title", default="Live capture")
a = ap.parse_args()
out = Path(__file__).resolve().parent.parent / "pulse" / "web" / "captures" / f"{a.name}.ndjson"


async def main():
    frames = []
    async with websockets.connect("ws://127.0.0.1:8765/stream", max_size=None) as ws:
        t_end = time.time() + a.seconds
        while time.time() < t_end:
            f = json.loads(await asyncio.wait_for(ws.recv(), 5))
            f.pop("client", None)
            frames.append(f)
    venues = sorted({k.split("/")[0] for f in frames for k in f["series"]})
    assert set(venues) <= {"binance", "coinbase"}, f"non-crypto source in capture: {venues}"
    head = {"capture": {"title": a.title, "format": 1, "frames": len(frames), "hz": 10, "venues": venues, "sources": "binance+coinbase public crypto feeds",
                        "start_ms": frames[0]["ts_ms"], "end_ms": frames[-1]["ts_ms"], "schema_version": frames[0]["v"]}}
    with out.open("w") as fh:
        fh.write(json.dumps(head, separators=(",", ":")) + "\n")
        for f in frames:
            fh.write(json.dumps(f, separators=(",", ":")) + "\n")
    print(f"wrote {out}  {len(frames)} frames  {out.stat().st_size / 1e6:.2f} MB  seq {frames[0]['seq']}..{frames[-1]['seq']}")

asyncio.run(main())
