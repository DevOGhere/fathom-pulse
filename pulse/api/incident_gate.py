"""Day 6 gate:  python -m pulse.api.incident_gate --runs 3 --seconds 8

Against the RUNNING server: provoke a real feed stall N times and check that, each time, (1) a stall incident opens for every feed within
OPEN_MAX_S of the induce call, (2) it is labelled induced and its cause says so, (3) it clears within CLEAR_MAX_S after the drop ends.
A run is INVALID (not a pass, not a fail of the detector) if a real incident was already open when we started: the feed was flapping.
"""
import argparse, json, sys, time, urllib.request

OPEN_MAX_S, CLEAR_MAX_S = 7.0, 10.0
U = "http://127.0.0.1:8765"
get = lambda p: json.load(urllib.request.urlopen(U + p, timeout=5))
post = lambda p: json.load(urllib.request.urlopen(urllib.request.Request(U + p, method="POST"), timeout=5))
ap = argparse.ArgumentParser(); ap.add_argument("--runs", type=int, default=3); ap.add_argument("--seconds", type=float, default=8)
a = ap.parse_args()
ok = invalid = 0
for r in range(a.runs):
    time.sleep(35)                                              # let a normal baseline refill and any previous incident clear
    pre = [i for i in get("/incidents") if i["end_ms"] is None]
    if pre:
        print(f"[{r + 1}] INVALID: real incident already open: {[(i['kind'], i['feed']) for i in pre]}"); invalid += 1; continue
    known = {i["id"] for i in get("/incidents")}
    t0 = time.time(); post(f"/incident/induce?seconds={a.seconds}")
    opened, cleared = {}, {}
    while time.time() - t0 < a.seconds + CLEAR_MAX_S + 6:
        for i in get("/incidents"):
            if i["id"] in known or i["kind"] != "stall": continue
            opened.setdefault(i["feed"], i)
            if i["end_ms"] is not None: cleared[i["feed"]] = i
        if len(cleared) >= 2: break
        time.sleep(0.5)
    feeds = sorted(opened)
    d_open = {f: round(opened[f]["start_ms"] / 1000 - t0, 1) for f in feeds}
    d_clear = {f: round(cleared[f]["end_ms"] / 1000 - (t0 + a.seconds), 1) for f in cleared}
    good = (len(opened) == 2 and all(o["induced"] and o["cause"].startswith("Induced") for o in opened.values())
            and all(0 <= v <= OPEN_MAX_S for v in d_open.values()) and len(cleared) == 2 and all(-1 <= v <= CLEAR_MAX_S for v in d_clear.values()))
    ok += good
    print(f"[{r + 1}] feeds {feeds}  opened after {d_open} s  cleared {d_clear} s after the drop ended  induced={[o['induced'] for o in opened.values()]}  {'PASS' if good else 'FAIL'}")
print(f"\npass {ok}/{a.runs}  invalid(feed flapping) {invalid}")
print("RESULT:", "PASS" if ok == a.runs else ("INCONCLUSIVE" if ok + invalid == a.runs and ok else "FAIL"))
sys.exit(0 if ok == a.runs else 1)
