"""FastAPI app: WS /stream (10Hz JSON snapshots), GET /health, GET / (bare counter page)."""
from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
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


app.mount("/static", StaticFiles(directory=WEB), name="static")      # tokens.css lives at /static/ds/tokens.css
_debug_stats: dict = {}


@app.get("/")
async def index():
    return FileResponse(WEB / "index.html")


@app.get("/counter")
async def counter():
    return FileResponse(WEB / "counter.html")


@app.post("/debug/stats")
async def post_stats(stats: dict):
    """Browser-side render meter posts here so the 60fps gate can be proven on stdout (debug only)."""
    _debug_stats.update(stats)
    return {"ok": True}


@app.get("/debug/stats")
async def get_stats():
    return _debug_stats


@app.post("/calibrate")
async def calibrate(stage: int = 0):
    """Run one self-test now (stage 2 or 3; 0 = random). Blocks ~11 s, returns the outcome record."""
    return await hub.calibrator.run_once(stage or None)


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


@app.get("/health")
async def health():
    return {"seq": hub.seq, "clients": len(hub.clients), "ingest": hub.ingest_stats(),
            "tick_error": hub.last_tick_error, "keys": [f"{v}/{s}" for v, s in hub.agg.keys()]}


@app.websocket("/stream")
async def stream(ws: WebSocket, throttle_ms: int = 0):
    await ws.accept()
    client = hub.add_client(min(max(throttle_ms, 0), 2000) / 1000)

    async def sender() -> None:
        while True:
            frame = await client.queue.get()
            if client.throttle_s:
                await asyncio.sleep(client.throttle_s)
            client.sent += 1
            await ws.send_text(json.dumps({**frame, "client": {"drops": client.drops, "sent": client.sent, "queue_max": 2, "sent_ms": time.time_ns() / 1e6}},
                                          separators=(",", ":")))

    task = asyncio.create_task(sender())
    try:
        while True:
            await ws.receive_text()      # we never expect input; this just detects the disconnect
    except WebSocketDisconnect:
        pass
    finally:
        task.cancel()
        hub.remove_client(client)
