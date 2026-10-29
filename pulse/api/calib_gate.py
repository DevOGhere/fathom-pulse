"""Day 4 gate:  python -m pulse.api.calib_gate --runs 10

Fires N real self-tests (alternating stage 2/3) against a running server and prints each outcome. PASS needs N consecutive
passes, every sham reporting "nothing found", and a record of the size error against its noise-derived tolerance.
(The two negative controls — wrong expected stage must fail, sham with a real injection must be a false alarm — are unit
tests in tests/test_calib_math.py.)
"""
import argparse, json, sys, urllib.request

def run(stage):
    req = urllib.request.Request(f"http://127.0.0.1:8765/calibrate?stage={stage}", method="POST")
    return json.load(urllib.request.urlopen(req, timeout=60))

ap = argparse.ArgumentParser(); ap.add_argument("--runs", type=int, default=10); n = ap.parse_args().runs
oks = shams = 0; errs = []
for i in range(n):
    r = run(2 if i % 2 == 0 else 3)
    oks += bool(r["ok"]); shams += bool(r["sham_ok"]); errs.append((r["stage"], r["size_err_ms"], r["tol_ms"]))
    print(f"[{i + 1:2d}/{n}] inject stage {r['stage']} {r['d_ms']:.0f} ms -> found {r['found']}  delta={ (r['deltas_ms'] or {}).get(str(r['stage'])) }  err={r['size_err_ms']} tol={r['tol_ms']}  z_sep={r['z_sep']}  sham_ok={r['sham_ok']}  {'PASS' if r['ok'] else 'FAIL: ' + str(r['reason'])}", flush=True)
print(f"\nreal passes {oks}/{n} (consecutive required)  sham clean {shams}/{n}")
for st in (2, 3):
    e = [abs(x[1]) for x in errs if x[0] == st and x[1] is not None]
    if e: print(f"stage {st}: mean |size error| {sum(e) / len(e):.2f} ms over {len(e)} runs")
print("RESULT:", "PASS" if oks == n and shams == n else "FAIL")
sys.exit(0 if oks == n and shams == n else 1)
