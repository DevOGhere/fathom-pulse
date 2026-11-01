"""Replay determinism gate:  .venv/bin/python tools/replay_check.py <scratch_dir> <capture_url_path> [frames,...]

Two COLD headless Chromes (fresh profile each), the replay page in manual mode, jump to the same frame counts, hash the six canvases
plus the key text panels with SHA-256. PASS = every hash identical across the two cold loads, AND the hashes differ between frame
counts (a hash that never changes would prove nothing). Needs the server running (it only serves the static page + capture file).
"""
import asyncio, json, sys, subprocess, time, urllib.request, shutil
import websockets

D, URLPATH = sys.argv[1], sys.argv[2]
NS = [int(x) for x in (sys.argv[3] if len(sys.argv) > 3 else "100,450,900").split(",")]
CH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


async def one(port, profile):
    p = subprocess.Popen([CH, "--headless=new", "--disable-gpu", "--no-sandbox", f"--user-data-dir={profile}", f"--remote-debugging-port={port}", "--window-size=1600,1250", "about:blank"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(40):
            try:
                pg = [t for t in json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/json")) if t["type"] == "page"][0]; break
            except Exception:
                time.sleep(0.5)
        async with websockets.connect(pg["webSocketDebuggerUrl"], max_size=None) as ws:
            n = 0
            async def call(m, **params):
                nonlocal n; n += 1; i = n; await ws.send(json.dumps({"id": i, "method": m, "params": params}))
                while True:
                    r = json.loads(await ws.recv())
                    if r.get("id") == i: return r
            async def ev(expr):
                r = await call("Runtime.evaluate", expression=expr, awaitPromise=True, returnByValue=True)
                if "exceptionDetails" in r.get("result", {}): raise RuntimeError(r["result"]["exceptionDetails"]["exception"]["description"][:300])
                return r["result"]["result"].get("value")
            await call("Runtime.enable"); await call("Page.enable")
            await call("Emulation.setDeviceMetricsOverride", width=1600, height=1250, deviceScaleFactor=1, mobile=False)
            await call("Page.navigate", url=f"http://127.0.0.1:8765/?replay={URLPATH}&manual=1")
            for _ in range(60):
                if await ev("!!window.__pulse"): break
                await asyncio.sleep(0.5)
            info = await ev("JSON.stringify({n: __pulse.n, title: __pulse.head.title})")
            out = [await ev(f"__pulse.run({k})") for k in NS]
            dims = await ev("JSON.stringify(Object.fromEntries(Object.entries(cvs).map(([k, v]) => [k, [v.w, v.h, v.c.width]])))")
            print("  canvas dims", port, dims)
            import base64, os
            if os.environ.get("DUMP"):
                open(f"{os.environ['DUMP']}_{port}.png", "wb").write(base64.b64decode((await ev("$('cMid').toDataURL()")).split(",")[1]))
            return json.loads(info), out
    finally:
        p.terminate(); p.wait()


async def main():
    (i1, h1), (i2, h2) = await one(9341, f"{D}/rc1"), await one(9342, f"{D}/rc2")
    print("capture:", i1)
    h1, h2 = [json.loads(x) for x in h1], [json.loads(x) for x in h2]
    for k, a, b in zip(NS, h1, h2):
        print(f"  frame {k:>4}: {a['all'][:16]}  {b['all'][:16]}  {'IDENTICAL' if a['all'] == b['all'] else 'DIFFERENT'}")
        for name in a['parts']:
            if a['parts'][name] != b['parts'][name]:
                print(f"      differs: {name}   {a['text'].get(name, '(canvas)')!r}  vs  {b['text'].get(name, '(canvas)')!r}")
    same = all(a['all'] == b['all'] for a, b in zip(h1, h2)); distinct = len({a['all'] for a in h1}) == len(h1)
    print("distinct across frame counts:", distinct)
    print("RESULT:", "PASS" if same and distinct else "FAIL")
    sys.exit(0 if same and distinct else 1)

asyncio.run(main())
