"""The main demo video, audio-first:  .venv/bin/python tools/make_demo.py <work_dir> <out.mp4> [--tempo 1.08] [--voice-id <id>]

Every line of narration is synthesised first (ElevenLabs convert_with_timestamps), so every word has a real time. Then each scene is PLANNED from those
times: captions appear, buttons are clicked and diagram parts are revealed on the word that is being spoken (anchors below), and a scene lasts as long as
its narration needs. tools/make_video.py records the scenes with that plan (--timing), the voice track is built from the same schedule, and
tools/polish_video.py adds the music bed and the sounds. The narration complements the screen: the page shows facts, the voice says what they mean.
Key from the project's .env (loaded before the SDK; never printed). Lines are cached by content, so a re-run costs no credits.
"""
import json, os, subprocess, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
import make_reel as MR      # noqa: E402  (synth_line, find, dur)

LEAD0, GAP, TAIL = 0.5, 0.30, 1.6          # first line starts 0.5 s into a scene; gap between lines; silence kept after the last line

# diagram pages: (literal in the page, template with § for the time, event). Times come from the words, so the animation follows the voice.
DIAGRAM = {
    "journey": [("animation-delay:7.5s", "animation-delay:§s", "b1"), ("animation-delay:12.3s", "animation-delay:§s", "b2"),
                ("animation-delay:14.0s", "animation-delay:§s", "b3"), ("animation-delay:16.2s", "animation-delay:§s", "b4"),
                ("in .8s 19s forwards", "in .8s §s forwards", "note"), ('dur="16s" begin="0.6s"', 'dur="§s" begin="0.6s"', "dotdur")],
    "selftest": [("#a .verdict{animation-delay:4s}", "#a .verdict{animation-delay:§s}", "vA"), ("#b{top:470px;animation-delay:6s}", "#b{top:470px;animation-delay:§s}", "pB"),
                 ("animation:grow 2.4s 8.2s forwards", "animation:grow 2.4s §s forwards", "gB"), ("animation:in .5s 7.6s forwards", "animation:in .5s §s forwards", "inj"),
                 ("#b .verdict{animation-delay:11.2s}", "#b .verdict{animation-delay:§s}", "vB"), ("animation:in .8s 15s forwards}", "animation:in .8s §s forwards}", "foot")],
    "detector": [("#c2{top:540px;animation-delay:7.5s}", "#c2{top:540px;animation-delay:§s}", "c2"), ("animation-delay:2.4s", "animation-delay:§s", "q1"),
                 ("animation:climb 4s 9.5s forwards", "animation:climb 4s §s forwards", "climb"), ("#c1 .cause{animation:in .7s 3.4s forwards}", "#c1 .cause{animation:in .7s §s forwards}", "cause1"),
                 ("#c2 .cause{animation:in .7s 12s forwards}", "#c2 .cause{animation:in .7s §s forwards}", "cause2"), ("animation:in .8s 16s forwards}.foot b", "animation:in .8s §s forwards}.foot b", "foot")],
}

# lines: dict(text, anchors={event: word | "@start"}, after=(event, seconds)); caps/clicks/js/keys/sfx name an event (or "start"/"end").
SCENES = [
    dict(name="title", lines=[dict(text="FathomPulse. A dashboard that measures how late it is, and tests itself.")]),
    dict(name="whole", caps=[("start", "Live: Binance and Coinbase BTC")],
         lines=[dict(text="This is live data, not a recording. Two exchanges, through a real Kafka pipeline.")]),
    dict(name="journey", diag="journey", sfx=[("b1", "click", 0.7, 0), ("b2", "click", 0.7, 0), ("b3", "click", 0.7, 0), ("b4", "click", 0.7, 0)], lines=[
        dict(text="Follow one update. Fathom's producer stamps it, and Kafka carries it to the processor."),
        dict(text="Rebuilding the order book and deriving the features is stage one. That part is Fathom's.", anchors={"b1": "stage"}),
        dict(text="Pulse consumes the result and stamps it again: stage two. Then a third, inside Pulse.", anchors={"b2": "stage", "b3": "third"}),
        dict(text="The last stage, to your screen, is only an estimate, and the page says so.", anchors={"b4": "last", "note": "estimate"})]),
    dict(name="age", caps=[("start", "Data age, per venue")],
         lines=[dict(text="Each chip says how old that data is. If a feed dies, it says so out loud, instead of drawing an empty chart.")]),
    dict(name="anatomy", caps=[("start", "Anatomy of a Tick"), ("click", "Click a bar: pause and inspect")], clicks=[("click", "#cAnat", 0.62, 0.55)], keys=[("end", "Escape")],
         sfx=[("click", "click", 1.0, 0)], lines=[
        dict(text="Every update is split into measured stages, with real timestamps."),
        dict(text="Click any bar, and that second freezes: every record, its Kafka offset, and its size.", anchors={"click": "click"})]),
    dict(name="slo", caps=[("start", "p99 against a 100 ms target")],
         lines=[dict(text="Judged against a hundred milliseconds at p99. The ten hertz frame wait is kept out, because that is display, not processing.")]),
    dict(name="selftest", diag="selftest", sfx=[("pB", "click", 0.7, 0), ("gB", "click", 0.7, 0)], lines=[
        dict(text="How can a dashboard check itself? First, a sham run with nothing hidden: it must find nothing.", anchors={"vA": "find"}),
        dict(text="Then eighty milliseconds go into stage two.", anchors={"pB": "then", "inj": "eighty", "gB": "eighty"}),
        dict(text="The waterfall must show them there, and nowhere else.", anchors={"vB": "nowhere"}),
        dict(text="It checks Pulse's own stages. Not the exchange, not the network.", anchors={"foot": "checks"})]),
    dict(name="calib", caps=[("start", "Calibration self-test"), ("c2s", "Sham window"), ("c3s", "Real delay injected"), ("c4s", "Verdict (it expires)")],
         clicks=[("click", "#btnTest", 0.5, 0.5)], sfx=[("click", "click", 1.0, 0), ("click", "chime", 0.9, 10.2)], lines=[
        dict(text="Now the real thing, live.", anchors={"click": "live"}),
        dict(text="First, a quiet sham window, to prove it does not cry wolf.", after=("click", 1.2), anchors={"c2s": "@start"}),
        dict(text="Now eighty milliseconds go into one of its own stages.", after=("click", 5.8), anchors={"c3s": "@start"}),
        dict(text="It found it, and the verdict expires, so it cannot stay green by default.", after=("click", 10.6), anchors={"c4s": "@start"})]),
    dict(name="detector", diag="detector", lines=[
        dict(text="When a feed goes quiet, the cause depends on the evidence."),
        dict(text="If the processor's queue is flat, the gap is upstream, and Pulse says it cannot tell where.", anchors={"q1": "flat", "cause1": "upstream"}),
        dict(text="If the queue is climbing, the delay is at or inside the processor.", anchors={"c2": "climbing", "climb": "climbing", "cause2": "inside"}),
        dict(text="I tested this by really freezing the processor. The first version of this rule got it wrong.", anchors={"foot": "tested"})]),
    dict(name="induce", caps=[("start", "Induced stall: 8 s"), ("i2s", "Detector opens an incident"), ("i3s", "Cleared")], clicks=[("click", "#btnInduce", 0.5, 0.5)],
         sfx=[("click", "click", 1.0, 0), ("click", "alert", 0.8, 2.4), ("click", "chime", 0.7, 8.6)], lines=[
        dict(text="Now I break the feed on purpose.", anchors={"click": "break"}),
        dict(text="For eight seconds Pulse drops every record. The detector opens an incident, and labels it induced.", after=("click", 1.6), anchors={"i2s": "@start"}),
        dict(text="When the feed returns, the incident clears.", after=("click", 8.6), anchors={"i3s": "@start"})]),
    dict(name="market", caps=[("start", "Market lens")], js=[("tape", "window.__ann.hl(null,'Top of book')")], lines=[
        dict(text="The market side: mid price, microprice and order-book imbalance."),
        dict(text="And the tape. Aggressor side comes from Fathom's buyer-maker flag.", anchors={"tape": "tape"})]),
    dict(name="replay", caps=[("start", "Replay: recorded, not live")],
         lines=[dict(text="The same page can replay a recording, through the same code path. Two cold browsers render it byte for byte identically.")]),
    dict(name="end", lines=[dict(text="Millisecond-class by design. Not a microsecond system, and it says so.")]),
]
ORDER = [s["name"] for s in SCENES]


def main():
    a = sys.argv[1:]
    work, out = Path(a[0]), Path(a[1])
    tempo = float(a[a.index("--tempo") + 1]) if "--tempo" in a else 1.08
    voice = a[a.index("--voice-id") + 1] if "--voice-id" in a else "cjVigY5qzO86Huf0OWal"
    work.mkdir(parents=True, exist_ok=True)

    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")                                   # BEFORE importing elevenlabs
    from elevenlabs import ElevenLabs
    c = ElevenLabs()

    # 1. synthesise every line (neighbouring lines are passed as context so the tone carries across requests)
    flat = [L["text"] for s in SCENES for L in s["lines"]]
    k, synth = 0, {}
    for s in SCENES:
        for L in s["lines"]:
            synth[id(L)] = MR.synth_line(c, voice, tempo, work, L["text"], flat[k - 1] if k else "", flat[k + 1] if k + 1 < len(flat) else "")
            k += 1

    # 2. plan each scene from the word times
    timing, events_out, voice_plan = {}, [], {}
    for s in SCENES:
        t, E, starts = LEAD0, {"start": 0.4}, []
        for L in s["lines"]:
            inf = synth[id(L)]
            st = t if not starts else t + GAP
            if "after" in L: st = max(st, E[L["after"][0]] + L["after"][1])
            starts.append(st)
            for name, word in L.get("anchors", {}).items():
                E[name] = st + (0.0 if word == "@start" else MR.find(inf["ws"], word)[1])
            t = st + inf["n"]
        dur = round(t + TAIL, 2)
        E["end"] = dur - 1.0
        if "b4" in E: E["dotdur"] = max(3.0, E["b4"] - 0.6)
        o = {"dur": dur}
        if s.get("caps"): o["caps"] = [[round(E[e], 2), txt] for e, txt in s["caps"]]
        if s.get("clicks"): o["clicks"] = [[round(E[e], 2), sel, fx, fy] for e, sel, fx, fy in s["clicks"]]
        if s.get("js"): o["js"] = [[round(E[e], 2), expr] for e, expr in s["js"]]
        if s.get("keys"): o["keys"] = [[round(E[e], 2), key] for e, key in s["keys"]]
        if s.get("diag"):
            page = (HERE / "diagrams" / f"{s['diag']}.html").read_text()
            for lit, tpl, ev in DIAGRAM[s["diag"]]:
                assert page.count(lit) == 1, (s["diag"], lit, page.count(lit))
                page = page.replace(lit, tpl.replace("§", f"{E[ev]:.2f}"))
            (work / f"{s['diag']}_built.html").write_text(page)
            o["url"] = (work / f"{s['diag']}_built.html").resolve().as_uri()
            events_out.append([s["name"], 0.0, "whoosh", 0.6])
        timing[s["name"]] = o
        for ev, sfx, gain, delta in s.get("sfx", []): events_out.append([s["name"], round(E[ev] + delta, 2), sfx, gain])
        voice_plan[s["name"]] = [(st, synth[id(L)]) for st, L in zip(starts, s["lines"])]
        print(f"  {s['name']:9} {dur:5.1f}s  lines {len(starts)}")
    (work / "timing.json").write_text(json.dumps(timing))
    (work / "events.json").write_text(json.dumps(events_out))

    # 3. record every scene with that plan
    subprocess.run([sys.executable, str(HERE / "make_video.py"), str(work / "rec"), str(work / "rec_all.mp4"), "--only", ",".join(ORDER), "--timing", str(work / "timing.json")],
                   check=True, stdout=subprocess.DEVNULL)
    vid = work / "rec" / "vid"

    # 4. picture, voice track from the same schedule (offsets use the recorded scene lengths), subtitles
    (work / "scenes.txt").write_text("".join(f"file '{(vid / (n + '.mp4')).resolve()}'\n" for n in ORDER))
    picture = work / "picture.mp4"
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", str(work / "scenes.txt"), "-c", "copy", str(picture)], check=True)
    total = MR.dur(picture)
    inputs, filt, cues, t0, n = [], [], [], 0.0, 0
    for name in ORDER:
        for st, inf in voice_plan[name]:
            inputs += ["-i", str(inf["wav"])]; ms = int((t0 + st) * 1000)
            filt.append(f"[{n}:a]adelay={ms}|{ms}[a{n}]"); cues.append((t0 + st, t0 + st + inf["n"], inf)); n += 1
        t0 += MR.dur(vid / f"{name}.mp4")
    filt.append("".join(f"[a{i}]" for i in range(n)) + f"amix=inputs={n}:normalize=0,apad=whole_dur={total:.3f},atrim=0:{total:.3f}[v]")
    voice_wav = work / "voice.wav"
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", *inputs, "-filter_complex", ";".join(filt), "-map", "[v]", "-ar", "44100", "-ac", "1", str(voice_wav)], check=True)
    narrated = work / "narrated.mp4"
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(picture), "-i", str(voice_wav), "-map", "0:v", "-map", "1:a", "-c:v", "copy", "-c:a", "aac", "-b:a", "160k", str(narrated)], check=True)
    texts = [L["text"] for s in SCENES for L in s["lines"]]
    ts = lambda x: f"{int(x // 3600):02d}:{int(x % 3600 // 60):02d}:{x % 60:06.3f}"
    out.with_suffix(".vtt").write_text("WEBVTT\n\n" + "".join(f"{ts(a0)} --> {ts(b0)}\n{txt}\n\n" for (a0, b0, _), txt in zip(cues, texts)))

    # 5. music bed and sounds at the planned moments
    subprocess.run([sys.executable, str(HERE / "polish_video.py"), str(narrated), str(vid), str(out), "--order", ",".join(ORDER), "--events-json", str(work / "events.json")], check=True)
    print(f"done: {out}  ({total:.1f}s)")


if __name__ == "__main__":
    main()
