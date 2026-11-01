"""Real downstream stall for the demo and for the detector's real-world check:  .venv/bin/python tools/chaos.py [seconds<=10] [--dry-run]

Pauses Fathom's host-side processor with SIGSTOP, then resumes it with SIGCONT. Nothing is edited, deleted or reconfigured; no synthetic
data: the broker keeps filling, lag climbs for real, and Pulse's detector has to notice it on its own (these incidents are NOT labelled induced).
Safety: the pause is capped at 10 s (a longer one risks a consumer-group rebalance), SIGCONT is sent on every exit path (Ctrl-C, SIGTERM,
exceptions), and it refuses to run unless exactly one processor.consumer process exists.
"""
import json, os, signal, subprocess, sys, time, urllib.request

U = "http://127.0.0.1:8765"
get = lambda p: json.load(urllib.request.urlopen(U + p, timeout=3))
args = [a for a in sys.argv[1:] if not a.startswith("--")]; dry = "--dry-run" in sys.argv
secs = min(10.0, max(2.0, float(args[0]) if args else 8.0))
import re
ps = subprocess.run(["ps", "-Ao", "pid=,command="], capture_output=True, text=True).stdout.splitlines()
pids = [l.split(None, 1)[0] for l in ps if re.match(r"\s*\d+\s+\S*[Pp]ython\S*\s+-m\s+processor\.consumer\s*$", l)]     # the process itself, not a shell that merely mentions it
if len(pids) != 1: sys.exit(f"expected exactly one processor.consumer process, found {len(pids)}: {pids}")
pid = int(pids[0]); paused = False


def resume(*_):
    global paused
    if paused:
        os.kill(pid, signal.SIGCONT); paused = False; print("SIGCONT sent")
    if _: sys.exit(1)


for sg in (signal.SIGINT, signal.SIGTERM): signal.signal(sg, resume)
real_before = {i["id"] for i in get("/incidents")}
print(f"processor pid {pid}; pausing for {secs:.0f} s{' (dry run: nothing will be paused)' if dry else ''}")
if dry: sys.exit(0)
t0 = time.time(); peak = 0; seen = {}
try:
    os.kill(pid, signal.SIGSTOP); paused = True
    while time.time() - t0 < secs: time.sleep(0.5)
finally:
    resume()
print("resumed; watching recovery for up to 40 s")
frames = 0
while time.time() - t0 < secs + 40:
    try:
        for i in get("/incidents"):
            if i["id"] not in real_before: seen[i["id"]] = i
        import asyncio, websockets
        async def lag():
            async with websockets.connect("ws://127.0.0.1:8765/stream", max_size=None) as ws:
                f = json.loads(await ws.recv()); return f["lag"].get("total"), f["pulse_age_ms"]
        lg, age = asyncio.run(lag()); peak = max(peak, lg or 0)
        print(f"  t+{time.time() - t0:4.0f}s  lag={lg}  pulse_age={age} ms  incidents={len(seen)}", end="\r")
        if time.time() - t0 > secs + 4 and (lg or 0) < 30 and all(i["end_ms"] for i in seen.values()) and (age or 0) < 1000: break
    except Exception as e:
        print("  (probe error:", type(e).__name__, ")", end="\r")
    time.sleep(1)
print(f"\npeak lag {peak} msgs")
for i in sorted(seen.values(), key=lambda x: x["start_ms"]):
    dur = "open" if not i["end_ms"] else f"{(i['end_ms'] - i['start_ms']) / 1000:.0f} s"
    print(f"  {i['kind']:<10} {i['feed'] or 'processor':<18} {'INDUCED' if i['induced'] else 'REAL   '} {dur:>5}  {i['cause'][:110]}")
