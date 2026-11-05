"""The reel, audio-first:  .venv/bin/python tools/make_reel.py <work_dir> <out_prefix> [--tempo 1.08] [--voice-id <id>] [--video-music]

1. Each narration line is synthesised with ElevenLabs `convert_with_timestamps` (eleven_v4): the audio AND the time every character is spoken.
2. Lines are scheduled one after another; every on-screen reveal is tied to a WORD (ANCHORS) and gets that word's real start time,
   so "Twice" turns red while the voice says "Twice", the strike-through runs on "Wrong", the dots fall while "ten times in seventeen minutes" is spoken.
3. tools/diagrams/reel.html is the template (timings as CSS variables); it is recorded with tools/make_video.py at 1080x1920.
4. The voice track is built from the same schedule, muxed onto the picture, and tools/polish_video.py adds music and sounds.
   --video-music additionally makes a second version whose music AND sound effects come from ElevenLabs `music.video_to_music` (the silent picture is
   uploaded, no mp3 involved), to compare against the first (own music prompt + sounds placed at the anchor times).
The key comes from the project's .env (loaded BEFORE the SDK is imported; never printed). Everything is cached by content in <work_dir>.
"""
import base64, hashlib, json, os, re, subprocess, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

# panel -> its lines (indices into LINES); a panel appears just before its first line and leaves when the next panel's line starts
LINES = [
    dict(id="l0", panel=0, text="I built a lie detector for data pipelines. Then I caught it lying. Twice.", anchors={"twice": "twice"}),
    dict(id="l1", panel=1, text="Prices arrive from two exchanges. Fathom's producer stamps each one the moment it lands.", anchors={"n_ex": "prices", "n_prod": "producer"}),
    dict(id="l2", panel=1, text="Kafka carries it to the processor, which rebuilds the order book and derives the features. Stage one is Fathom's own.",
         anchors={"n_k1": "kafka", "n_proc": "processor", "n_k2": "derives", "br1": "stage"}),
    dict(id="l3", panel=1, text="Pulse reads that derived topic on its own, stamps it again, and bins it every second. Stages two and three.",
         anchors={"n_pulse": "pulse", "br2": "stages", "br3": "three"}),
    dict(id="l4", panel=1, text="The browser adds a fourth stage, labelled an estimate. Two checks watch the whole thing: a self-test, and an incident detector.",
         anchors={"n_br": "browser", "br4": "fourth", "checks": "checks"}),
    dict(id="l5", panel=2, text="It lied twice. Both lies are tests now. Measured, not assumed.", anchors={}),
]
GAP_SAME, GAP_NEXT, LEAD = 0.30, 0.55, 0.25      # seconds: between lines of one panel, between panels, how early a panel appears before its voice
# sound effects: (anchor, sfx, gain). Anchors resolve to the same word times as the page.
SFX_AT = [("twice", "alert", 0.7), ("p1in", "whoosh", 0.6), ("br1", "click", 0.8), ("br2", "click", 0.8), ("br3", "click", 0.8), ("br4", "click", 0.8), ("checks", "chime", 0.8), ("p2in", "chime", 0.7)]


def words(alignment):
    """characters + start/end seconds -> [(word, start, end)]"""
    ch, st, en = alignment["characters"], alignment["character_start_times_seconds"], alignment["character_end_times_seconds"]
    out, cur, s0, e0 = [], "", None, None
    for c, a, b in zip(ch, st, en):
        if c.isspace():
            if cur: out.append((cur, s0, e0)); cur = ""
            continue
        if not cur: s0 = a
        cur += c; e0 = b
    if cur: out.append((cur, s0, e0))
    return out


def norm(w):
    return re.sub(r"[^a-z']", "", w.lower())


def find(ws, key):
    """'wrong' = first match; 'both#2' = second occurrence"""
    name, _, occ = key.partition("#")
    occ = int(occ or 1)
    hits = [x for x in ws if norm(x[0]) == name]
    return hits[occ - 1]


def dur(p):
    return float(subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(p)], capture_output=True, text=True, check=True).stdout)


def synth_line(c, voice, tempo, work, text, prev, nxt):
    """One narration line -> {wav (tempo applied), ws: [(word, start, end)] in the sped-up timeline, n: seconds}. Cached by content."""
    from elevenlabs import VoiceSettings
    key = hashlib.sha1(f"ts|{voice}|{prev}|{text}|{nxt}".encode()).hexdigest()[:16]
    f = work / f"line_{key}.json"
    if not f.exists():
        r = c.text_to_speech.convert_with_timestamps(voice, text=text, model_id="eleven_v4", output_format="mp3_44100_128",
                                                     previous_text=prev or None, next_text=nxt or None,
                                                     voice_settings=VoiceSettings(stability=0.5, similarity_boost=0.75))
        d = r.model_dump() if hasattr(r, "model_dump") else dict(r)
        f.write_text(json.dumps({"audio": d.get("audio_base_64") or d.get("audio_base64"), "alignment": d["alignment"]}))
    j = json.loads(f.read_text())
    mp3 = work / f"line_{key}.mp3"; mp3.write_bytes(base64.b64decode(j["audio"]))
    wav = work / f"line_{key}.t.wav"
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(mp3), "-af", f"atempo={tempo}", "-ar", "44100", "-ac", "1", str(wav)], check=True)
    return dict(wav=wav, ws=[(w, s / tempo, e / tempo) for w, s, e in words(j["alignment"])], n=dur(wav))


def main():
    a = sys.argv[1:]
    work, prefix = Path(a[0]), Path(a[1])
    tempo = float(a[a.index("--tempo") + 1]) if "--tempo" in a else 1.08
    voice = a[a.index("--voice-id") + 1] if "--voice-id" in a else "cjVigY5qzO86Huf0OWal"       # Eric
    want_vm = "--video-music" in a
    work.mkdir(parents=True, exist_ok=True)

    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")                                  # BEFORE importing elevenlabs: the SDK reads the key at import time
    from elevenlabs import ElevenLabs, VoiceSettings
    c = ElevenLabs()

    # 1. synthesise with timestamps (cached)
    info = []
    for i, L in enumerate(LINES):
        prev = LINES[i - 1]["text"] if i else ""
        nxt = LINES[i + 1]["text"] if i + 1 < len(LINES) else ""
        key = hashlib.sha1(f"ts|{voice}|{prev}|{L['text']}|{nxt}".encode()).hexdigest()[:16]
        f = work / f"line_{key}.json"
        if not f.exists():
            r = c.text_to_speech.convert_with_timestamps(voice, text=L["text"], model_id="eleven_v4", output_format="mp3_44100_128",
                                                         previous_text=prev or None, next_text=nxt or None,
                                                         voice_settings=VoiceSettings(stability=0.5, similarity_boost=0.75))
            d = r.model_dump() if hasattr(r, "model_dump") else dict(r)
            f.write_text(json.dumps({"audio": d.get("audio_base_64") or d.get("audio_base64"), "alignment": d["alignment"]}))
        j = json.loads(f.read_text())
        mp3 = work / f"line_{key}.mp3"; mp3.write_bytes(base64.b64decode(j["audio"]))
        wav = work / f"line_{key}.t.wav"
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(mp3), "-af", f"atempo={tempo}", "-ar", "44100", "-ac", "1", str(wav)], check=True)
        info.append(dict(L, wav=wav, ws=[(w, s / tempo, e / tempo) for w, s, e in words(j["alignment"])], n=dur(wav)))

    # 2. schedule
    t, starts = 0.35, []
    for i, L in enumerate(info):
        if i:
            t += GAP_SAME if L["panel"] == info[i - 1]["panel"] else GAP_NEXT
        starts.append(t); t += L["n"]
    total = t + 1.9
    T = {}                                                       # anchor name -> absolute seconds
    for L, s0 in zip(info, starts):
        for name, word in L["anchors"].items():
            T[name] = s0 + find(L["ws"], word)[1]
    first = {}
    for L, s0 in zip(info, starts): first.setdefault(L["panel"], s0)
    for p in range(3):
        T[f"p{p}in"] = max(0.05, first[p] - LEAD)
        T[f"p{p}out"] = max(0.05, first[p + 1] - LEAD) if p + 1 in first else 999.0
    ONE = {k: f"{v:.2f}s" for k, v in T.items()}
    page = (HERE / "diagrams" / "reel.html").read_text().replace("@@VARS@@", ":root{" + "".join(f"--{k}:{v};" for k, v in ONE.items()) + "}")
    (work / "reel_built.html").write_text(page)
    print(f"schedule: {len(info)} lines, picture {total:.1f}s;  twice@{T['twice']:.2f}  n_proc@{T['n_proc']:.2f}  br2@{T['br2']:.2f}  checks@{T['checks']:.2f}")

    # 3. record the page
    env = dict(os.environ, REEL_URL=(work / "reel_built.html").resolve().as_uri(), REEL_DUR=f"{total:.2f}")
    subprocess.run([sys.executable, str(HERE / "make_video.py"), str(work / "rec"), str(work / "rec_all.mp4"), "--only", "reel", "--size", "1080x1920"], check=True, env=env, stdout=subprocess.DEVNULL)
    picture = work / "rec" / "vid" / "reel.mp4"

    # 4. voice track from the same schedule, muxed onto the picture
    inputs, filt = [], []
    for i, (L, s0) in enumerate(zip(info, starts)):
        inputs += ["-i", str(L["wav"])]; ms = int(s0 * 1000); filt.append(f"[{i}:a]adelay={ms}|{ms}[a{i}]")
    filt.append("".join(f"[a{i}]" for i in range(len(info))) + f"amix=inputs={len(info)}:normalize=0,apad=whole_dur={dur(picture):.3f},atrim=0:{dur(picture):.3f}[v]")
    voice_wav = work / "voice.wav"
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", *inputs, "-filter_complex", ";".join(filt), "-map", "[v]", "-ar", "44100", "-ac", "1", str(voice_wav)], check=True)
    narrated = work / "narrated.mp4"
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(picture), "-i", str(voice_wav), "-map", "0:v", "-map", "1:a", "-c:v", "copy", "-c:a", "aac", "-b:a", "160k", str(narrated)], check=True)
    vtt = ["WEBVTT\n"]
    ts = lambda x: f"{int(x // 3600):02d}:{int(x % 3600 // 60):02d}:{x % 60:06.3f}"
    for L, s0 in zip(info, starts): vtt.append(f"{ts(s0)} --> {ts(s0 + L['n'])}\n{L['text']}\n")
    Path(str(prefix) + ".vtt").write_text("\n".join(vtt))
    events = [["reel", round(T[an], 2), sfx, g] for an, sfx, g in SFX_AT]
    (work / "events.json").write_text(json.dumps(events))

    # 5. polish: A = own music prompt + sounds at the anchor times; B = ElevenLabs video_to_music (music and sfx from the silent picture)
    polish = [sys.executable, str(HERE / "polish_video.py")]
    os.makedirs(work / "scenes", exist_ok=True)
    (work / "scenes" / "reel.mp4").write_bytes(picture.read_bytes())
    subprocess.run([*polish, str(narrated), str(work / "scenes"), str(prefix) + "_A_own_music.mp4", "--order", "reel", "--events-json", str(work / "events.json")], check=True)
    if want_vm:
        subprocess.run([*polish, str(narrated), str(work / "scenes"), str(prefix) + "_B_video_to_music.mp4", "--order", "reel", "--video-music", str(picture), "--use-video-sfx"], check=True)


if __name__ == "__main__":
    main()
