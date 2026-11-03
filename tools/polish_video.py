"""Final polish on a narrated demo video: a quiet instrumental music bed (ducked under the voice) and a few subtle UI sounds.
    .venv/bin/python tools/polish_video.py <narrated.mp4> <scenes_dir> <out.mp4> [--music-volume 0.16] [--sfx-volume 0.35] [--no-music]

Everything is generated with ElevenLabs (key from the project's .env, loaded BEFORE the SDK is imported; never printed) and cached by prompt in
<out>_assets/, so a re-run costs nothing. The picture is copied untouched (-c:v copy). Sound effect times are the moments the recorded scenes
act (a click, an incident opening); they are listed in EVENTS so they can be moved when a scene is re-recorded.
"""
import hashlib, subprocess, sys
from pathlib import Path

ORDER = ["title", "whole", "journey", "age", "anatomy", "slo", "selftest", "calib", "detector", "induce", "market", "replay", "end"]

SFX = {   # name -> (prompt, seconds)
    "click": ("Soft subtle UI click, short, crisp, quiet, clean", 0.6),
    "alert": ("Gentle low notification tone, calm and brief warning, soft, not alarming", 1.2),
    "chime": ("Soft positive confirmation chime, very short, subtle, gentle", 1.2),
    "whoosh": ("Very subtle soft transition swoosh, airy, quiet, short", 1.0),
}
EVENTS = [   # (scene, seconds into the scene, sfx, gain)
    ("journey", 0.0, "whoosh", 0.6), ("selftest", 0.0, "whoosh", 0.6), ("detector", 0.0, "whoosh", 0.6),
    ("anatomy", 5.0, "click", 1.0),
    ("calib", 4.0, "click", 1.0), ("calib", 14.0, "chime", 0.9),
    ("induce", 2.0, "click", 1.0), ("induce", 4.6, "alert", 0.8), ("induce", 10.4, "chime", 0.7),
]
MUSIC_PROMPT = ("Calm minimal ambient electronic underscore for a technical product demo: soft warm pads, a very gentle low pulse, "
                "sparse light plucks, steady and unobtrusive, slow tempo, no vocals, no drums, no dramatic build")


def dur(p):
    return float(subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(p)], capture_output=True, text=True, check=True).stdout)


def main():
    a = sys.argv[1:]
    video, scenes, out = Path(a[0]), Path(a[1]), Path(a[2])
    mvol = float(a[a.index("--music-volume") + 1]) if "--music-volume" in a else 0.16
    svol = float(a[a.index("--sfx-volume") + 1]) if "--sfx-volume" in a else 0.35
    use_music = "--no-music" not in a
    assets = out.parent / (out.stem + "_assets"); assets.mkdir(parents=True, exist_ok=True)

    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")            # BEFORE importing elevenlabs: the SDK reads the key at import time
    from elevenlabs import ElevenLabs
    c = ElevenLabs()
    total = dur(video)

    def cached(kind, prompt, ext, make):
        f = assets / f"{kind}_{hashlib.sha1(prompt.encode()).hexdigest()[:12]}.{ext}"
        if not f.exists():
            f.write_bytes(b"".join(make()))
        return f

    starts, t = {}, 0.0
    for n in ORDER:
        starts[n] = t; t += dur(scenes / f"{n}.mp4")
    assert abs(t - total) < 0.5, f"scene lengths ({t:.1f}s) do not add up to the video ({total:.1f}s): re-assemble first"

    sfx_files = {k: cached("sfx", f"{k}|{p}|{s}", "mp3", lambda p=p, s=s: c.text_to_sound_effects.convert(text=p, duration_seconds=s, prompt_influence=0.6)) for k, (p, s) in SFX.items()}
    music = cached("music", MUSIC_PROMPT + f"|{int(total)}", "mp3", lambda: c.music.compose(prompt=MUSIC_PROMPT, music_length_ms=int((total + 2) * 1000), model_id="music_v2_5", force_instrumental=True)) if use_music else None

    inputs, filt, mix = ["-i", str(video)], [], []
    filt.append("[0:a]asplit=2[vo][vsc]")
    idx = 1
    if music:
        inputs += ["-i", str(music)]
        filt.append(f"[{idx}:a]aresample=44100,atrim=0:{total:.2f},afade=t=in:d=3,afade=t=out:st={total - 5:.2f}:d=5,volume={mvol}[m]")
        filt.append("[m][vsc]sidechaincompress=threshold=0.02:ratio=12:attack=20:release=700:makeup=1[md]")      # the bed dips whenever the voice speaks
        mix.append("[md]"); idx += 1
    else:
        filt[0] = "[0:a]anull[vo]"
    for j, (scene, off, name, gain) in enumerate(EVENTS):
        inputs += ["-i", str(sfx_files[name])]
        ms = int((starts[scene] + off) * 1000)
        filt.append(f"[{idx}:a]aresample=44100,volume={svol * gain:.3f},adelay={ms}|{ms}[s{j}]"); mix.append(f"[s{j}]"); idx += 1
    filt.append(f"[vo]{''.join(mix)}amix=inputs={1 + len(mix)}:normalize=0:duration=first,loudnorm=I=-16:TP=-1.5:LRA=11[aout]")
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", *inputs, "-filter_complex", ";".join(filt), "-map", "0:v", "-map", "[aout]",
                    "-c:v", "copy", "-c:a", "aac", "-b:a", "160k", str(out)], check=True)
    print(f"wrote {out} ({dur(out):.1f}s)  music={'yes' if music else 'no'}  sfx events={len(EVENTS)}")


main()
