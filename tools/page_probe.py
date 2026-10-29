"""Real-time page probe (Day 3 proof): python tools/page_probe.py <scratch_dir> <seconds> <out.png> [width]
Drives headless Chrome over DevTools (NOT --virtual-time-budget/--timeout: they break rAF and the websocket), loads the
Pulse page, prints page state + JS exceptions, saves a screenshot. Render-cost stats are then at GET /debug/stats.
<scratch_dir> holds the throwaway Chrome profile. Needs the server running (python -m pulse.api) and Google Chrome (macOS path).
"""
import asyncio, json, sys, base64, urllib.request, subprocess, time, os, signal
import websockets
D=sys.argv[1]; SECS=float(sys.argv[2]); OUT=sys.argv[3]; W=int(sys.argv[4]) if len(sys.argv)>4 else 1600
CH="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
p=subprocess.Popen([CH,"--headless=new","--disable-gpu","--no-sandbox",f"--user-data-dir={D}/chp_cdp","--remote-debugging-port=9333",f"--window-size={W},1250","about:blank"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
async def main():
    for _ in range(40):
        try:
            tabs=json.load(urllib.request.urlopen("http://127.0.0.1:9333/json")); 
            pg=[t for t in tabs if t["type"]=="page"][0]; break
        except Exception: time.sleep(0.5)
    async with websockets.connect(pg["webSocketDebuggerUrl"],max_size=None) as ws:
        n=0; msgs=[]
        async def call(m,**params):
            nonlocal n; n+=1; i=n; await ws.send(json.dumps({"id":i,"method":m,"params":params}))
            while True:
                r=json.loads(await ws.recv())
                if r.get("id")==i: return r
                ev(r)
        def ev(r):
            if r.get("method")=="Runtime.consoleAPICalled" and r["params"]["type"] in("error","warning"): msgs.append(("console."+r["params"]["type"],str(r["params"]["args"])[:200]))
            if r.get("method")=="Runtime.exceptionThrown": msgs.append(("EXCEPTION",r["params"]["exceptionDetails"].get("exception",{}).get("description","")[:300]))
        await call("Runtime.enable"); await call("Page.enable")
        await call("Emulation.setDeviceMetricsOverride",width=W,height=1250,deviceScaleFactor=1,mobile=False)
        await call("Page.navigate",url="http://127.0.0.1:8765/")
        t0=time.time()
        while time.time()-t0<SECS:
            try: ev(json.loads(await asyncio.wait_for(ws.recv(),timeout=1)))
            except asyncio.TimeoutError: pass
        r=await call("Runtime.evaluate",expression="JSON.stringify({draws:S.draws,msgs:S.msgs,missed:S.seqGaps,sel:S.sel,log:S.log.length,connected:S.connected,raf:S.rafTicks})",returnByValue=True)
        print("page state:",r["result"]["result"].get("value"))
        s=await call("Page.captureScreenshot",format="png"); open(OUT,"wb").write(base64.b64decode(s["result"]["data"]))
        print("js problems:",msgs or "none")
asyncio.run(main()); p.terminate()
