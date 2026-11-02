"""FastAPI app: WS /stream (10Hz JSON snapshots), GET /health, GET / (bare counter page)."""
from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from pulse.api import features as fv
from pulse.api.hub import Hub

WEB = Path(__file__).resolve().parent.parent / "web"
hub = Hub()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await hub.start()
    yield
    await hub.stop()


app = FastAPI(title="FathomPulse", lifespan=lifespan)


def _public() -> bool:
    """PULSE_PUBLIC=1 = a hosted build: the test-only and debug endpoints do not exist. Read per call so tests can flip it."""
    return os.environ.get("PULSE_PUBLIC") == "1"


def _max_clients() -> int:
    return int(os.environ.get("PULSE_MAX_CLIENTS", "10"))


def _guard_mutation(request: Request) -> None:
    """State-changing endpoints: absent in a public build, and never callable from another website.
    A browser always sends Origin on a cross-site POST, so a tab on evil.example cannot make this page's server drop its feed.
    No Origin (curl, our own tools) and same-origin (the page itself) pass; this is not authentication."""
    if _public():
        raise HTTPException(404, "not available in a public build")
    origin = request.headers.get("origin")
    if origin and urlparse(origin).netloc != request.headers.get("host"):
        raise HTTPException(403, "cross-origin request refused")


@app.middleware("http")
async def no_stale_assets(request, call_next):
    """Dev server: the page's own files must never come from a browser cache (a stale app.css hid a whole feature once).
    Also the baseline security headers: no framing, no MIME sniffing, no referrer leakage."""
    resp = await call_next(request)
    if request.url.path == "/" or request.url.path.startswith("/static/"):
        resp.headers["Cache-Control"] = "no-cache"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "no-referrer"
    return resp


app.mount("/static", StaticFiles(directory=WEB), name="static")      # tokens.css lives at /static/ds/tokens.css
_debug_stats: dict = {}


@app.get("/")
async def index():
    return FileResponse(WEB / "index.html")


@app.get("/counter")
async def counter():
    return FileResponse(WEB / "counter.html")


@app.post("/debug/stats")
async def post_stats(stats: dict, request: Request):
    """Browser-side render meter posts here so the 60fps gate can be proven on stdout (debug only; absent when PULSE_PUBLIC=1)."""
    _guard_mutation(request)
    _debug_stats.update(stats)
    return {"ok": True}


@app.get("/debug/stats")
async def get_stats():
    if _public():
        raise HTTPException(404, "not available in a public build")
    return _debug_stats


@app.post("/calibrate")
async def calibrate(request: Request, stage: int = 0):
    """Run one self-test now (stage 2 or 3; 0 = random). Blocks ~11 s, returns the outcome record."""
    _guard_mutation(request)
    return await hub.calibrator.run_once(stage or None)


@app.post("/incident/induce")
async def incident_induce(request: Request, seconds: float = 8.0):
    """Incident demo: drop all feed records for N seconds (3-30). The detector must open an incident labelled induced, then clear it."""
    _guard_mutation(request)
    return hub.induce_incident(seconds)


@app.get("/incidents")
async def incidents():
    return hub.detector.snapshot()


@app.get("/calibration")
async def calibration():
    return hub.calibrator.snapshot()


def _fv_bins(venue: str, symbol: str):
    if (venue, symbol) not in hub.agg.keys():
        raise HTTPException(404, f"unknown venue/symbol; known: {[f'{v}/{s}' for v, s in hub.agg.keys()]}")
    return hub.agg.bins(venue, symbol)


@app.get("/features/latest")
async def features_latest(venue: str, symbol: str):
    """pulse.fv.1: the most recent CLOSED 1 s bin as a feature vector."""
    v = fv.latest(_fv_bins(venue, symbol), int(time.time() * 1000))
    if v is None:
        raise HTTPException(404, "no closed bin yet")
    return v


@app.get("/features/window")
async def features_window(venue: str, symbol: str, from_ms: int | None = None, to_ms: int | None = None):
    """pulse.fv.1: closed bins with close in (from_ms, to_ms], oldest first. Ring only (<= 5 min); identical shape to /latest."""
    return {"schema_version": fv.SCHEMA_VERSION, "retention": "ring<=300x1s",
            "vectors": fv.window(_fv_bins(venue, symbol), int(time.time() * 1000), from_ms, to_ms)}


@app.get("/tick")
async def tick(venue: str, symbol: str, sec: int):
    """Inspector: one closed second's bin plus every record in it with its own stage durations. Ring only (~5 min)."""
    recs = hub.agg.records_for_second(venue, symbol, sec)
    if recs is None:
        raise HTTPException(404, "unknown venue/symbol")
    b = next((x for x in hub.agg.bins(venue, symbol) if x.start_s == sec), None)
    if b is None and not recs:
        raise HTTPException(404, "that second is outside the in-memory window (about the last 5 minutes)")
    return {"venue": venue, "symbol": symbol, "sec": sec, "topic": f"market.derived.{venue}.{symbol}", "bin": b.to_dict() if b else None, "records": recs,
            "note": "Pulse wall clock for times; stage durations are differences of host monotonic stamps; records come from the in-memory ring"}


@app.get("/health")
async def health():
    return {"seq": hub.seq, "clients": len(hub.clients), "ingest": hub.ingest_stats(),
            "tick_error": hub.last_tick_error, "keys": [f"{v}/{s}" for v, s in hub.agg.keys()]}


_ser: dict = {"frame": None, "text": ""}      # one-entry cache: every client sends the same frame, so it is serialised once, not once per client


def frame_text(frame: dict, client_block: dict) -> str:
    """Same bytes as json.dumps({**frame, "client": client_block}, compact), but the (large) frame part is encoded once per tick."""
    if _ser["frame"] is not frame:
        _ser["frame"], _ser["text"] = frame, json.dumps(frame, separators=(",", ":"))[:-1]
    return _ser["text"] + ',"client":' + json.dumps(client_block, separators=(",", ":")) + "}"


@app.websocket("/stream")
async def stream(ws: WebSocket, throttle_ms: int = 0):
    await ws.accept()
    if len(hub.clients) >= _max_clients():
        await ws.close(code=1013)                 # try again later: a connection flood must not starve the 10 Hz ticker
        return
    client = hub.add_client(min(max(throttle_ms, 0), 2000) / 1000)

    async def sender() -> None:
        while True:
            frame = await client.queue.get()
            if client.throttle_s:
                await asyncio.sleep(client.throttle_s)
            client.sent += 1
            await ws.send_text(frame_text(frame, {"drops": client.drops, "sent": client.sent, "queue_max": 2, "sent_ms": time.time_ns() / 1e6}))

    task = asyncio.create_task(sender())
    try:
        while True:
            await ws.receive_text()      # we never expect input; this just detects the disconnect
    except WebSocketDisconnect:
        pass
    finally:
        task.cancel()
        hub.remove_client(client)
