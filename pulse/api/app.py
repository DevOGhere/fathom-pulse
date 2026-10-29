"""FastAPI app: WS /stream (10Hz JSON snapshots), GET /health, GET / (bare counter page)."""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from pulse.api.hub import Hub

WEB = Path(__file__).resolve().parent.parent / "web"
hub = Hub()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await hub.start()
    yield
    await hub.stop()


app = FastAPI(title="FathomPulse", lifespan=lifespan)


@app.get("/")
async def index():
    return FileResponse(WEB / "index.html")


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
            await ws.send_text(json.dumps({**frame, "client": {"drops": client.drops, "sent": client.sent, "queue_max": 2}},
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
