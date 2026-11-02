"""Put a voiceover on an EXISTING demo video without touching the picture:
    .venv/bin/python tools/add_voice.py <video.mp4> <scenes_dir> <out.mp4> [--engine say|kokoro] [--voice-name Daniel] [--rate 170]
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

ORDER = ["title", "whole", "age", "anatomy", "slo", "calib", "induce", "market", "replay", "end"]
START = 0.5                      # own-recording files start this far into their scene

# scene -> [(offset_s, text)]. Offsets follow the captions/clicks in make_video.py's scenes(); each line must end before the next begins.
SEGMENTS = {
    "title": [(0.8, "FathomPulse. A live dashboard on Fathom's Kafka pipeline that measures how late it is, and tests itself.")],
    "whole": [(0.5, "Most dashboards pretend to be real-time. This one streams Binance and Coinbase Bitcoin through Kafka, and measures every step.")],
    "age": [(0.5, "Every number carries its age: Binance ten updates a second, Coinbase sixteen. If a feed dies, it says how long ago.")],
    "anatomy": [(0.5, "One update's life, split into measured stages. Real timestamps, not estimates."),
                (7.0, "Click a bar to pause on that second, and see each record's Kafka offset.")],
    "slo": [(0.5, "Latency is judged against a hundred millisecond target at the ninety-ninth percentile. The frame wait is shown separately, as a display choice.")],
    "calib": [(0.5, "Here is the idea I care about. Pulse hides an eighty millisecond delay inside one of its own stages, and checks whether the waterfall finds the right stage and size."),
              (14.0, "A quiet sham run comes first, to prove the test does not cry wolf."),
              (30.0, "It found it. It covers Pulse's own stages, not the exchange or the network.")],
    "induce": [(0.5, "Now I break the feed on purpose. Pulse discards every record for eight seconds."),
               (6.5, "The detector opens an incident, labels it induced, and says only what it can see."),
               (15.5, "When the feed returns, it clears.")],
    "market": [(0.5, "The market side: mid and microprice, order book imbalance, top of book and trades."),
               (6.5, "Aggressor side comes from Fathom's own buyer-maker flag.")],
    "replay": [(0.5, "The same page and code path can replay a recording. Two cold browsers render it byte for byte identically.")],
    "end": [(1.0, "It is millisecond-class by design. Not a microsecond system, and it says so.")],
}


def dur(path):
    return float(subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)], capture_output=True, text=True, check=True).stdout)


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
    engine = a[a.index("--engine") + 1] if "--engine" in a else "say"
    voice = a[a.index("--voice-name") + 1] if "--voice-name" in a else ("af_heart" if engine == "kokoro" else "Daniel")
    model_dir = a[a.index("--model-dir") + 1] if "--model-dir" in a else "."
    rate = int(a[a.index("--rate") + 1]) if "--rate" in a else 170
    own = Path(a[a.index("--dir") + 1]) if "--dir" in a else None
    work = out.parent / (out.stem + "_audio"); work.mkdir(parents=True, exist_ok=True)

    pieces, t0, report = [], 0.0, []          # pieces: (abs_start_s, wav)
    for name in ORDER:
        d = dur(scenes / f"{name}.mp4")
        if own:
            hits = [f for f in own.glob(f"{name}.*") if f.suffix.lower() in (".wav", ".m4a", ".mp3", ".aiff", ".aif", ".flac", ".ogg")]
            if not hits: sys.exit(f"no recording for scene '{name}' in {own}")
            length = dur(hits[0]); pieces.append((t0 + START, hits[0])); report.append((name, d, START + length, None))
        else:
            ends = []
            for i, (off, text) in enumerate(SEGMENTS[name]):
                w = work / f"{name}_{i}.wav"; (kokoro_to_wav(text, w, voice, model_dir) if engine == "kokoro" else say_to_wav(text, w, voice, rate))
                ln = dur(w); pieces.append((t0 + off, w)); ends.append((off, off + ln))
            for (o1, e1), (o2, _) in zip(ends, ends[1:]):
                if e1 > o2: print(f"  WARN {name}: a line ends at {e1:.1f}s but the next starts at {o2:.1f}s")
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
    print(f"wrote {out}  (video {dur(video):.1f}s, audio {total:.1f}s, picture stream copied untouched)")


main()
