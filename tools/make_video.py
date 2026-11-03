"""Guided demo video from the REAL page:  .venv/bin/python tools/make_video.py <scratch_dir> <out.mp4> [--only N,N] [--base http://127.0.0.1:8765]
                                                                              [--voice none|say|dir:<folder>] [--voice-name Daniel] [--rate 170]
Narration (--voice): audio comes FIRST and drives the timing: each scene is held for max(its planned length, narration + 1.2 s), so the
picture never ends before the sentence does. `say` = macOS built-in voice (no key, no download, robotic). `dir:<folder>` = your own
recordings, one file per scene named after it (title.wav, whole.m4a, ...; any format ffmpeg reads). An API voice (ElevenLabs, OpenAI TTS)
plugs in at synth() below: one function that turns text into an audio file. The spoken text is NARRATION[...]; it may only say what the page shows.

Drives headless Chrome over DevTools (same approach as page_probe.py), records each scene with Page.startScreencast,
overlays captions and a highlight box by injecting DOM into the page (so frames arrive already annotated), then stitches
the scenes with ffmpeg. Nothing is mocked: every scene is the running app (or a replay of a recorded capture, captioned as such).
Scenes marked live act on Pulse only (calibration, "Induce a stall"); none of them touch Fathom's processor.
Needs: server running (python -m pulse.api), Google Chrome (macOS path), ffmpeg on PATH.
"""
import asyncio, base64, json, os, subprocess, sys, time, urllib.request, shutil
from pathlib import Path
import websockets

W, H = 1600, 1000
if "--size" in sys.argv:                                  # e.g. --size 1080x1920 for a vertical reel
    W, H = (int(v) for v in sys.argv[sys.argv.index("--size") + 1].split("x"))
CH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PORT = 9334

OVERLAY = r"""
(() => {
  if (window.__ann) return;
  const st = document.createElement('style');
  st.textContent = `#annCap{position:fixed;left:50%;bottom:28px;transform:translateX(-50%);max-width:1180px;z-index:99999;
    background:rgba(8,10,12,.92);color:#f4f4f2;font:600 24px/1.35 -apple-system,Helvetica,Arial,sans-serif;padding:16px 26px;
    border-radius:14px;border:1px solid rgba(255,255,255,.18);box-shadow:0 8px 30px rgba(0,0,0,.5);text-align:center;transition:opacity .25s;pointer-events:none}   /* clicks pass through: a caption once covered the button the scene was clicking */
    #annHl{position:fixed;z-index:99998;border:3px solid #ffd54a;border-radius:14px;box-shadow:0 0 0 9999px rgba(0,0,0,.38);
    pointer-events:none;transition:all .35s}`;
  document.head.append(st);
  const cap = document.createElement('div'); cap.id = 'annCap'; cap.style.opacity = 0; document.body.append(cap);
  const hl = document.createElement('div'); hl.id = 'annHl'; hl.style.display = 'none'; document.body.append(hl);
  window.__ann = {
    cap(t) { cap.textContent = t || ''; cap.style.opacity = t ? 1 : 0; },
    hl(sel, text) {
      let el = null;
      if (sel) el = document.querySelector(sel);
      if (!el && text) { const h = [...document.querySelectorAll('h3')].find(x => x.offsetParent && x.textContent.trim().toLowerCase().startsWith(text.toLowerCase())); el = h && (h.closest('.card') || h.parentElement); }
      if (!el) { hl.style.display = 'none'; return false; }
      el.scrollIntoView({ block: 'nearest' });
      const r = el.getBoundingClientRect(), p = 6;
      Object.assign(hl.style, { display: 'block', left: r.left - p + 'px', top: r.top - p + 'px', width: r.width + 2 * p + 'px', height: r.height + 2 * p + 'px' });
      return true;
    },
    off() { hl.style.display = 'none'; },
  };
})();
"""

CARD = lambda title, sub: ("data:text/html;charset=utf-8," + urllib.parse.quote(
    f"<body style='margin:0;background:#0e1113;color:#f4f4f2;font-family:-apple-system,Helvetica,Arial,sans-serif;display:flex;"
    f"flex-direction:column;align-items:center;justify-content:center;height:100vh;text-align:center'>"
    f"<div style='font-size:64px;font-weight:700;letter-spacing:-.5px'>{title}</div>"
    f"<div style='font-size:28px;margin-top:22px;color:#a9b0b5;max-width:1100px;line-height:1.4'>{sub}</div></body>"))

import urllib.parse   # noqa: E402  (CARD uses it at call time)

DIAG = lambda n: (Path(__file__).resolve().parent / "diagrams" / f"{n}.html").as_uri()      # animated explainer pages, recorded like any scene

# What is SAID per scene (docs/DEMO.md rule: nothing the page does not show). Captions stay on screen; this is the voice.
NARRATION = {
    "title": "FathomPulse. A live dashboard on Fathom's Kafka pipeline that measures how late it is, and tests itself.",
    "whole": "Most dashboards pretend to be real-time. This one shows Binance and Coinbase Bitcoin flowing through a Kafka pipeline, and measures every step.",
    "age": "Every number carries its age. Binance updates about ten times a second, Coinbase about sixteen. If a feed dies, this says how long ago the last update was, instead of showing an empty chart.",
    "anatomy": "One update's life, split into measured stages: producer to derived, derived to Pulse, Pulse to frame. Real timestamps, not estimates. Click a bar to pause on that second and see every record, with its Kafka partition, offset and size. The last stage, to your screen, is labelled a local estimate.",
    "slo": "Pipeline latency is judged against a one hundred millisecond target at the ninety-ninth percentile. The wait for the next ten hertz frame is shown separately, because that is a display choice, not processing.",
    "calib": "Here is the idea I care about. Pulse hides an eighty millisecond delay inside one of its own stages, and checks whether the waterfall finds the right stage and size. A quiet sham run comes first, to prove the test does not cry wolf. It covers Pulse's own stages, not the exchange or the network.",
    "induce": "Now I break the feed on purpose. Pulse discards every record for eight seconds. The detector opens an incident, labels it induced, and says only what it can actually see. When the feed returns, it clears.",
    "market": "The market side: mid price and microprice, order book imbalance, top of book and trades. The aggressor side comes from Fathom's own buyer-maker flag.",
    "replay": "The same page, and the same code path, can replay a recording. Two cold browsers render it byte for byte identically.",
    "end": "It is millisecond-class by design. Not a microsecond system, and it says so.",
}


def synth(text, out_wav, voice_name, rate):
    """text -> audio file. Swap this body for an API voice (ElevenLabs / OpenAI TTS): read the key from an env var, never from the repo."""
    aiff = Path(str(out_wav) + ".aiff")
    subprocess.run(["say", "-v", voice_name, "-r", str(rate), "-o", str(aiff), text], check=True)
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(aiff), "-ar", "44100", "-ac", "1", str(out_wav)], check=True)
    aiff.unlink()


def audio_for(name, mode, outdir, voice_name, rate):
    """Returns (audio_path or None, seconds). Audio exists before the picture is recorded because it decides how long the scene lasts."""
    if mode == "none":
        return None, 0.0
    if mode == "say":
        path = Path(outdir) / f"{name}.voice.wav"
        synth(NARRATION[name], path, voice_name, rate)
    else:                                                    # dir:<folder>, the person's own recordings
        hits = [f for f in Path(mode[4:]).glob(f"{name}.*") if f.suffix.lower() in (".wav", ".m4a", ".mp3", ".aiff", ".aif", ".flac", ".ogg")]
        if not hits:
            raise SystemExit(f"--voice dir: no recording for scene '{name}' in {mode[4:]} (expected {name}.wav / .m4a / ...)")
        path = hits[0]
    dur = float(subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)], capture_output=True, text=True, check=True).stdout)
    return path, dur


def mux(mp4, audio, outdir):
    """Put the voice on the scene (0.5 s in, padded with silence to the picture's length). A silent track when there is no voice, so every scene has the same streams and concat can copy."""
    out = Path(outdir) / (mp4.stem + ".av.mp4")
    a_in = ["-i", str(audio), "-filter_complex", "[1:a]adelay=500|500,apad[a]"] if audio else ["-f", "lavfi", "-i", "anullsrc=r=44100:cl=mono", "-filter_complex", "[1:a]anull[a]"]
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(mp4), *a_in, "-map", "0:v", "-map", "[a]", "-shortest", "-c:v", "copy", "-c:a", "aac", "-ar", "44100", "-ac", "1", str(out)], check=True)
    return out


# (url, seconds, [(t, caption)], opts). opts: hl=(css, h3-text), js=[(t, expr)], until=expr (poll, ends scene early), card=True
def scenes(base):
    live = base + "/"
    return [
        dict(name="title", url=CARD("FathomPulse v1.0", "A live dashboard on Fathom's Kafka pipeline that measures how late it is, and tests itself."), dur=4.5, caps=[]),
        dict(name="whole", url=live, dur=9, caps=[(0.5, "Live: Binance and Coinbase BTC, streamed through Fathom's Kafka pipeline into this page.")]),
        dict(name="journey", url=DIAG("journey"), dur=24, caps=[]),
        dict(name="reel", url=os.environ.get("REEL_URL") or DIAG("reel"), dur=float(os.environ.get("REEL_DUR", 42)), caps=[]),   # make_reel.py points REEL_URL at its built page
        dict(name="age", url=live, dur=10, hl=("#strip", None), caps=[(0.5, "Every number carries its age. Feed health per venue: Binance about 10 updates a second, Coinbase about 16."),
                                                                  (6, "If a feed dies, this reads 'last update N s ago', never a blank chart.")]),
        dict(name="anatomy", url=live, dur=15, hl=(None, "Anatomy of a Tick"),
             caps=[(0.5, "Anatomy of a Tick: one second of updates, split into measured stages."), (6, "Click a bar to pause on that second: Kafka partition:offset, bytes and raw stamps per record."), (12, "Esc resumes live.")],
             clicks=[(5, "#cAnat", 0.62, 0.55)], keys=[(14, "Escape")]),
        dict(name="slo", url=live, dur=10, hl=(None, "Latency distribution"),
             caps=[(0.5, "p99 pipeline latency against a 100 ms target."), (5, "The wait for the next 10 Hz frame is reported separately: a display choice, not processing.")]),
        dict(name="selftest", url=DIAG("selftest"), dur=23, caps=[]),
        dict(name="calib", url=live, dur=26, hl=(None, "Calibration"),
             caps=[(0.4, "Calibration: Pulse hides an 80 ms delay inside one of its own stages and checks whether the waterfall finds the right stage and size."),
                   (5.0, "A quiet 'sham' window runs first, to check it does not cry wolf."), (9.4, "Now the real delay goes in."),
                   (15.4, "The verdict expires; stale, it says so instead of staying green.")],
             clicks=[(4, "#btnTest", 0.5, 0.5)]),
        dict(name="detector", url=DIAG("detector"), dur=25.5, caps=[]),
        dict(name="induce", url=live, dur=24, hl=(None, "Incidents"),
             caps=[(0.5, "Now I break the feed on purpose: Pulse discards every record for 8 seconds."), (6, "The detector opens an incident and states only what Pulse can see."), (15, "It clears when the feed returns. Induced events are always labelled.")],
             clicks=[(2, "#btnInduce", 0.5, 0.5)]),
        dict(name="market", url=live, dur=11, hl=(None, "Mid & microprice"),
             caps=[(0.5, "The market side: mid, microprice, order-book imbalance, top of book and trades."), (6, "Trade aggressor comes from Fathom's own buyer-maker flag.")],
             js=[(6, "window.__ann.hl(null,'Top of book')")]),
        dict(name="replay", url=base + "/?replay=/static/captures/demo-90s.ndjson", dur=14, caps=[(0.5, "Same page, same code path, from a recording of the live feed. Replay is deterministic.")]),
        dict(name="end", url=CARD("Ms-class by design", "Not a microsecond system, and it says so. Recorded in headless Chrome on the same laptop that runs the pipeline."), dur=5, caps=[]),
    ]


class CDP:
    def __init__(self, ws): self.ws, self.n, self.frames, self.rest = ws, 0, [], []
    async def call(self, m, **p):
        self.n += 1; i = self.n
        await self.ws.send(json.dumps({"id": i, "method": m, "params": p}))
        while True:
            r = json.loads(await self.ws.recv())
            if r.get("id") == i: return r
            await self.on(r)
    async def on(self, r):
        if r.get("method") == "Page.screencastFrame":
            q = r["params"]; self.frames.append((time.monotonic(), base64.b64decode(q["data"])))
            self.n += 1; await self.ws.send(json.dumps({"id": self.n, "method": "Page.screencastFrameAck", "params": {"sessionId": q["sessionId"]}}))
    async def pump(self, secs):
        end = time.monotonic() + secs
        while (left := end - time.monotonic()) > 0:
            try: await self.on(json.loads(await asyncio.wait_for(self.ws.recv(), timeout=min(left, .2))))
            except asyncio.TimeoutError: pass
    async def ev(self, js): return (await self.call("Runtime.evaluate", expression=js, returnByValue=True)).get("result", {}).get("result", {}).get("value")
    async def click(self, sel, fx, fy):
        await self.ev(f"(()=>{{const e=document.querySelector({json.dumps(sel)}); if(e){{const b=e.getBoundingClientRect(); if(b.bottom>innerHeight-70||b.top<0) e.scrollIntoView({{block:'center', behavior:'instant'}})}}}})()")      # only when the target is off-screen or under the sticky status bar; instant, because a smooth scroll leaves stale coordinates
        await self.pump(0.25)
        r = await self.ev(f"(()=>{{const e=document.querySelector({json.dumps(sel)}); if(!e) return null; const b=e.getBoundingClientRect(); return [b.left+b.width*{fx}, b.top+b.height*{fy}]}})()")
        if not r: print("  (click target missing:", sel, ")"); return
        for t in ("mouseMoved", "mousePressed", "mouseReleased"):
            await self.call("Input.dispatchMouseEvent", type=t, x=r[0], y=r[1], button="left", clickCount=1)
    async def key(self, k):
        for t in ("keyDown", "keyUp"): await self.call("Input.dispatchKeyEvent", type=t, key=k, code=k, windowsVirtualKeyCode=27 if k == "Escape" else 0)


async def record(c, sc, outdir):
    await c.call("Page.navigate", url=sc["url"]); await c.pump(1.6)
    c.frames.clear()
    await c.ev(OVERLAY)
    if sc.get("hl"): await c.ev(f"window.__ann.hl({json.dumps(sc['hl'][0])},{json.dumps(sc['hl'][1])})")
    await c.call("Page.startScreencast", format="jpeg", quality=82, everyNthFrame=1)
    t0 = time.monotonic()
    todo = sorted([(t, "cap", x) for t, x in sc.get("caps", [])] + [(t, "click", (s, fx, fy)) for t, s, fx, fy in sc.get("clicks", [])] +
                  [(t, "js", e) for t, e in sc.get("js", [])] + [(t, "key", k) for t, k in sc.get("keys", [])], key=lambda x: x[0])
    until, done_at = sc.get("until"), None
    while (el := time.monotonic() - t0) < sc["dur"]:
        while todo and todo[0][0] <= el:
            _, kind, x = todo.pop(0)
            if kind == "cap": await c.ev(f"window.__ann.cap({json.dumps(x)})")
            elif kind == "click":
                await c.click(*x)
                if sc.get("hl"): await c.ev(f"window.__ann.hl({json.dumps(sc['hl'][0])},{json.dumps(sc['hl'][1])})")      # the click may have scrolled the page: re-place the highlight
            elif kind == "js": await c.ev(x)
            elif kind == "key": await c.key(x)
        if until and el > 4 and done_at is None and await c.ev(until): done_at = el
        if done_at is not None and el > done_at + sc.get("tail", 4): break
        await c.pump(0.08)
    await c.call("Page.stopScreencast"); await c.pump(0.2)
    end = time.monotonic()
    fr = c.frames[:]
    if not fr: raise SystemExit(f"scene {sc['name']}: no frames")
    d = Path(outdir) / sc["name"]; shutil.rmtree(d, ignore_errors=True); d.mkdir(parents=True)
    lst = []
    for i, (t, b) in enumerate(fr):
        (d / f"{i:05d}.jpg").write_bytes(b)
        nxt = fr[i + 1][0] if i + 1 < len(fr) else end
        lst += [f"file '{d / f'{i:05d}.jpg'}'", f"duration {max(nxt - t, 0.001):.4f}"]
    lst.append(f"file '{d / f'{len(fr) - 1:05d}.jpg'}'")
    (d / "list.txt").write_text("\n".join(lst))
    mp4 = Path(outdir) / f"{sc['name']}.mp4"
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", str(d / "list.txt"), "-vf",
                    f"fps=30,scale={W}:{H}:force_original_aspect_ratio=decrease,pad={W}:{H}:(ow-iw)/2:(oh-ih)/2:color=black,"
                    "fade=t=in:st=0:d=0.3,format=yuv420p", "-c:v", "libx264", "-crf", "20",
                    "-t", f"{end - fr[0][0]:.3f}", str(mp4)], check=True)     # -t: the concat demuxer's repeated last entry would otherwise count the final frame's duration twice
    print(f"  {sc['name']}: {len(fr)} frames, {end - t0:.1f}s -> {mp4.name}")
    return mp4


async def main():
    a = sys.argv[1:]; scratch, out = Path(a[0]), Path(a[1])
    only = set(a[a.index("--only") + 1].split(",")) if "--only" in a else None       # scene indices or names (--only journey,selftest)
    base = a[a.index("--base") + 1] if "--base" in a else "http://127.0.0.1:8765"
    voice = a[a.index("--voice") + 1] if "--voice" in a else "none"
    voice_name = a[a.index("--voice-name") + 1] if "--voice-name" in a else "Daniel"
    rate = int(a[a.index("--rate") + 1]) if "--rate" in a else 170
    (scratch / "vid").mkdir(parents=True, exist_ok=True)
    p = subprocess.Popen([CH, "--headless=new", "--no-sandbox", f"--user-data-dir={scratch}/chp_vid", f"--remote-debugging-port={PORT}",
                          f"--window-size={W},{H}", "about:blank"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(60):
            try: pg = [t for t in json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}/json")) if t["type"] == "page"][0]; break
            except Exception: time.sleep(0.5)
        async with websockets.connect(pg["webSocketDebuggerUrl"], max_size=None) as ws:
            c = CDP(ws)
            await c.call("Page.enable"); await c.call("Runtime.enable")
            await c.call("Emulation.setDeviceMetricsOverride", width=W, height=H, deviceScaleFactor=1, mobile=False)
            parts = []
            for i, sc in enumerate(scenes(base)):
                if only and str(i) not in only and sc["name"] not in only: continue
                print(f"[{i}] {sc['name']}")
                aud, adur = audio_for(sc["name"], voice, scratch / "vid", voice_name, rate)
                if aud: sc["dur"] = max(sc["dur"], adur + 1.2)         # narration decides the length, never the other way round
                mp4 = await record(c, sc, scratch / "vid")
                parts.append(mux(mp4, aud, scratch / "vid") if voice != "none" else mp4)
        (scratch / "vid" / "all.txt").write_text("\n".join(f"file '{m}'" for m in parts))
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", str(scratch / "vid" / "all.txt"), "-c", "copy", str(out)], check=True)
        print("wrote", out)
    finally:
        p.terminate()

asyncio.run(main())
