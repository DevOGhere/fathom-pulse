"""Put a voiceover on an EXISTING demo video without touching the picture:
    .venv/bin/python tools/add_voice.py <video.mp4 | -> <scenes_dir> <out.mp4> [--engine say|kokoro|elevenlabs] [--voice-name Daniel] [--rate 170]
                                       [--model-dir <folder with kokoro-v1.0.onnx + voices-v1.0.bin>] [--dir <own recordings>]

<scenes_dir> holds the per-scene mp4s that make_video.py wrote (title.mp4, whole.mp4, ...): their durations give the scene boundaries.
The video stream is copied (-c:v copy), only an audio track is added. Each SEGMENT says one thing at an offset (seconds into its scene),
placed so it lines up with what is on screen when it is said. The spoken text may only say what the page shows.
Voices: --engine kokoro = Kokoro-82M (open source, Apache-2.0, runs locally, no key; needs `pip install kokoro-onnx soundfile` in the python that runs
this script plus the two model files from github.com/thewh1teagle/kokoro-onnx releases "model-files-v1.0"; voices like af_heart, am_michael, bf_emma, bm_george),
macOS `say` (default, robotic, no key) or --dir <folder> with your own recordings named after the scene (title.m4a, whole.wav, ...;
each file starts 0.5 s into its scene, so record it as one take per scene; the segments below are your script). An API voice would replace
say_to_wav(); its key must come from an environment variable, never from the repo.
"""
import json, subprocess, sys
from pathlib import Path

ORDER = ["title", "whole", "journey", "age", "anatomy", "slo", "selftest", "calib", "detector", "induce", "market", "replay", "end"]
START = 0.5                      # own-recording files start this far into their scene

# scene -> [(offset_s, text)]. Offsets follow the captions/clicks in make_video.py's scenes(); each line must end before the next begins.
SEGMENTS = {
    "title": [(0.8, "FathomPulse. A live dashboard on Fathom's Kafka pipeline that measures how late it is, and tests itself.")],
    "whole": [(0.5, "Most dashboards pretend to be real-time. This one streams Binance and Coinbase Bitcoin through Kafka, and measures every step.")],
    "age": [(0.5, "Every number carries its age: Binance ten updates a second, Coinbase sixteen. If a feed dies, it says how long ago.")],
    "anatomy": [(0.5, "One update's life, split into measured stages. Real timestamps, not estimates."),
                (7.0, "Click a bar to pause on that second, and see each record's Kafka offset.")],
    "slo": [(0.5, "Latency is judged against a hundred millisecond target at the ninety-ninth percentile. The frame wait is shown separately, as a display choice.")],
    "reel": [(0.25, "I built a lie detector for data pipelines."),
             (3.2, "Live prices flow through Kafka. Pulse timestamps every hop, and explains any stall."),
             (9.6, "So I broke it on purpose, and waited for its verdict."),
             (14.2, "Confident. Specific. Wrong. The evidence was right there."),
             (19.6, "The patch cried wolf, ten times in seventeen minutes."),
             (23.6, "A fix is just a new hypothesis."),
             (27.1, "Both lies were mine, not the data's."),
             (30.3, "The rule now: never alarm on a number alone. Wait until something actually hurts. Both bugs live on as tests."),
             (38.5, "Measured. Not assumed.")],
    "journey": [(0.8, "Here is one update's journey. Fathom's producer stamps it, and Kafka carries it to the processor."),
                (7.6, "Stage one is Fathom's own work: rebuilding the book and deriving the features."),
                (12.6, "Pulse consumes the derived record, and stamps it again."),
                (16.3, "Stage four, to your screen, is a local estimate, and it says so.")],
    "selftest": [(0.6, "How can a dashboard check itself?"),
                 (2.6, "First a sham run: it must find nothing."),
                 (7.0, "Then eighty milliseconds go into stage two."),
                 (10.6, "The waterfall must show them in stage two, and nowhere else."),
                 (15.8, "It checks Pulse's own stages, not the exchange or the network.")],
    "calib": [(0.4, "Pulse hides an eighty millisecond delay in its own stages."),
              (4.9, "A quiet sham window runs first, to prove it does not cry wolf."),
              (9.4, "Then the real delay goes in. The waterfall must find it."),
              (14.4, "It found it. And the verdict expires."),
              (17.8, "So it cannot stay green by default.")],
    "detector": [(0.6, "When a feed goes quiet, the cause depends on the evidence."),
                 (4.8, "Queue flat: the gap is upstream, and Pulse says it cannot tell where."),
                 (10.0, "Queue climbing: the delay is at or inside the processor."),
                 (16.2, "I tested this by really freezing the processor for eight seconds. The first version of this rule got it wrong.")],
    "induce": [(0.5, "Now I break the feed on purpose. Pulse discards every record for eight seconds."),
               (6.5, "The detector opens an incident, labels it induced, and says only what it can see."),
               (15.5, "When the feed returns, it clears."),
               (18.8, "Induced events are always labelled.")],
    "market": [(0.5, "The market side: mid, microprice, imbalance, top of book and trades."),
               (6.5, "Aggressor side comes from Fathom's buyer-maker flag.")],
    "replay": [(0.5, "The same page and code path can replay a recording. Two cold browsers render it byte for byte identically.")],
    "end": [(1.0, "It is millisecond-class by design. Not a microsecond system, and it says so.")],
}


TEMPO = 1.0


def speed_up(wav):
    """In place: atempo keeps the pitch. Applied to every synthesized line, so the schedule sees the real (shorter) durations."""
    if TEMPO == 1.0:
        return
    tmp = wav.with_suffix(".t.wav")
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(wav), "-af", f"atempo={TEMPO}", str(tmp)], check=True)
    tmp.replace(wav)


def dur(path):
    return float(subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)], capture_output=True, text=True, check=True).stdout)


_el = None


def eleven_to_wav(text, out, voice_id, prev, nxt, cache_dir):
    """ElevenLabs eleven_v4. Key: ELEVENLABS_API_KEY from the project's .env (never printed, never in code). Cached by content so a re-run
    spends no credits; previous_text/next_text = request stitching, so the tone carries across separately generated lines."""
    global _el
    import hashlib
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")           # BEFORE importing elevenlabs: the SDK reads the key at import time
    from elevenlabs import ElevenLabs, VoiceSettings
    key = hashlib.sha1(f"v4|{voice_id}|0.5|0.75|{prev}|{text}|{nxt}".encode()).hexdigest()[:16]
    cached = Path(cache_dir) / f"{key}.mp3"
    if not cached.exists():
        if _el is None:
            _el = ElevenLabs()
        audio = _el.text_to_speech.convert(text=text, voice_id=voice_id, model_id="eleven_v4", output_format="mp3_44100_128",
                                           previous_text=prev or None, next_text=nxt or None,
                                           voice_settings=VoiceSettings(stability=0.5, similarity_boost=0.75))
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        cached.write_bytes(b"".join(audio))
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(cached), "-ar", "44100", "-ac", "1", str(out)], check=True)


_kokoro = None


def kokoro_to_wav(text, out, voice, model_dir):
    global _kokoro
    import soundfile as sf
    from kokoro_onnx import Kokoro
    if _kokoro is None:
        _kokoro = Kokoro(str(Path(model_dir) / "kokoro-v1.0.onnx"), str(Path(model_dir) / "voices-v1.0.bin"))
    samples, sr = _kokoro.create(text, voice=voice, speed=1.0, lang="en-us")
    raw = out.with_suffix(".raw.wav"); sf.write(str(raw), samples, sr)
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(raw), "-ar", "44100", "-ac", "1", str(out)], check=True)
    raw.unlink()


def say_to_wav(text, out, voice, rate):
    aiff = out.with_suffix(".aiff")
    subprocess.run(["say", "-v", voice, "-r", str(rate), "-o", str(aiff), text], check=True)
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(aiff), "-ar", "44100", "-ac", "1", str(out)], check=True)
    aiff.unlink()


def main():
    a = sys.argv[1:]
    video, scenes, out = Path(a[0]), Path(a[1]), Path(a[2])
    if "--order" in a: ORDER[:] = a[a.index("--order") + 1].split(",")      # e.g. --order reel for the vertical reel
    global TEMPO
    TEMPO = float(a[a.index("--tempo") + 1]) if "--tempo" in a else 1.0       # e.g. 1.1: ElevenLabs v4 has no speed setting, so the voice is sped up with ffmpeg atempo
    engine = a[a.index("--engine") + 1] if "--engine" in a else "say"
    voice = a[a.index("--voice-name") + 1] if "--voice-name" in a else {"kokoro": "af_heart", "elevenlabs": "JBFqnCBsd6RMkjVDRZzb"}.get(engine, "Daniel")   # elevenlabs: voice id (default George)
    model_dir = a[a.index("--model-dir") + 1] if "--model-dir" in a else "."
    rate = int(a[a.index("--rate") + 1]) if "--rate" in a else 170
    own = Path(a[a.index("--dir") + 1]) if "--dir" in a else None
    work = out.parent / (out.stem + "_audio"); work.mkdir(parents=True, exist_ok=True)
    if str(video) == "-":                      # no existing video: join the per-scene mp4s in ORDER (same encode settings, so a stream copy)
        (work / "scenes.txt").write_text("".join(f"file '{(scenes / (n + '.mp4')).resolve()}'\n" for n in ORDER))
        video = work / "picture.mp4"
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", str(work / "scenes.txt"), "-c", "copy", str(video)], check=True)

    flat = [t for n in ORDER for _, t in SEGMENTS[n]]                      # neighbours for ElevenLabs request stitching
    pos = 0
    pieces, t0, report, cues = [], 0.0, [], []                 # cues: (start_s, end_s, text) for the .vtt subtitle file          # pieces: (abs_start_s, wav)
    for name in ORDER:
        d = dur(scenes / f"{name}.mp4")
        if own:
            hits = [f for f in own.glob(f"{name}.*") if f.suffix.lower() in (".wav", ".m4a", ".mp3", ".aiff", ".aif", ".flac", ".ogg")]
            if not hits: sys.exit(f"no recording for scene '{name}' in {own}")
            length = dur(hits[0]); pieces.append((t0 + START, hits[0])); report.append((name, d, START + length, None))
        else:
            ends, prev_end = [], -1.0
            for i, (off, text) in enumerate(SEGMENTS[name]):
                w = work / f"{name}_{i}.wav"; (kokoro_to_wav(text, w, voice, model_dir) if engine == "kokoro" else eleven_to_wav(text, w, voice, flat[pos - 1] if pos else "", flat[pos + 1] if pos + 1 < len(flat) else "", out.parent / "eleven_cache") if engine == "elevenlabs" else say_to_wav(text, w, voice, rate)); pos += 1
                speed_up(w)
                ln = dur(w)
                start = max(off, prev_end + 0.35)          # scheduler: a slower voice pushes a line later instead of talking over the previous one
                if start > off + 0.05: print(f"  note {name}[{i}]: pushed from {off:.1f}s to {start:.1f}s (previous line still speaking)")
                pieces.append((t0 + start, w)); cues.append((t0 + start, t0 + start + ln, text)); prev_end = start + ln; ends.append((start, prev_end))
            report.append((name, d, ends[-1][1], None))
        t0 += d
    for name, d, end, _ in report:
        print(f"  {name:8} scene {d:5.1f}s  speech ends {end:5.1f}s  {'OK' if end <= d - 0.3 else 'TOO LONG'}")
    total = t0

    inputs, filt = [], []
    for i, (start, w) in enumerate(pieces):
        inputs += ["-i", str(w)]; filt.append(f"[{i}:a]adelay={int(start * 1000)}|{int(start * 1000)}[a{i}]")
    mix = "".join(f"[a{i}]" for i in range(len(pieces)))
    filt.append(f"{mix}amix=inputs={len(pieces)}:normalize=0,apad=whole_dur={total:.3f},atrim=0:{total:.3f},loudnorm=I=-16:TP=-1.5:LRA=11[aout]")
    voice_wav = work / "voice.wav"
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", *inputs, "-filter_complex", ";".join(filt), "-map", "[aout]", "-ar", "44100", "-ac", "1", str(voice_wav)], check=True)
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(video), "-i", str(voice_wav), "-map", "0:v", "-map", "1:a", "-c:v", "copy", "-c:a", "aac", "-b:a", "128k", str(out)], check=True)
    if cues:                                                   # subtitles for the web player, timed from the same schedule as the audio
        ts = lambda x: f"{int(x // 3600):02d}:{int(x % 3600 // 60):02d}:{x % 60:06.3f}"
        out.with_suffix(".vtt").write_text("WEBVTT\n\n" + "".join(f"{ts(a)} --> {ts(b)}\n{t}\n\n" for a, b, t in cues))
    print(f"wrote {out}  (video {dur(video):.1f}s, audio {total:.1f}s, picture stream copied untouched)")


main()
