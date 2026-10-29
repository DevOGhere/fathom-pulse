"""Runtime wiring: ingest -> aggregator -> 10Hz ticker -> per-client queues.

Backpressure (locked): each client has an asyncio.Queue(maxsize=2); when full the OLDEST frame is dropped and the
client's drop counter increments. Drops lose smoothness, never state (frames are self-contained). The counter is
attached to every frame sent to that client and is displayed in the engine lens.
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

from pulse.api.frames import build_frame
from pulse.ingest.consumer import IngestConsumer
from pulse.ingest.lag import LagMonitor
from pulse.ingest.rings import Aggregator, DerivedRec, TradeRec

TICK_S = 0.1
QUEUE_MAX = 2


class Client:
    def __init__(self, throttle_s: float = 0.0) -> None:
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
        self.drops = 0
        self.sent = 0
        self.throttle_s = throttle_s     # DEBUG hook: simulates a slow client link for the backpressure test


class Hub:
    def __init__(self) -> None:
        self.agg = Aggregator()
        self.clients: set[Client] = set()
        self.seq = 0
        self.last_tick_error: Optional[str] = None
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._ic: Optional[IngestConsumer] = None
        self._lag = LagMonitor()
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._ic = IngestConsumer(loop, self._queue)
        self._ic.start()
        self._lag.start()
        self._tasks = [asyncio.create_task(self._pump()), asyncio.create_task(self._closer()), asyncio.create_task(self._ticker())]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self._ic:
            self._ic.stop()
        self._lag.stop()

    def add_client(self, throttle_s: float = 0.0) -> Client:
        c = Client(throttle_s)
        self.clients.add(c)
        return c

    def remove_client(self, c: Client) -> None:
        self.clients.discard(c)

    def ingest_stats(self) -> dict:
        ic = self._ic
        return {"queue_drops": ic.queue_drops if ic else 0, "late": self.agg.late,
                "errors": sum(ic.errors.values()) if ic else 0, "clients": len(self.clients),
                "seen": sum(ic.counts.values()) if ic else 0, "decoded": sum(ic.decoded.values()) if ic else 0,
                "poll_alive": bool(ic and ic._thread and ic._thread.is_alive())}

    async def _pump(self) -> None:
        while True:
            for r in await self._queue.get():
                if isinstance(r, DerivedRec):
                    self.agg.add_derived(r)
                elif isinstance(r, TradeRec):
                    self.agg.add_trade(r)

    async def _closer(self) -> None:
        while True:
            self.agg.close_due(time.time_ns())
            self.agg.assert_caps()
            await asyncio.sleep(0.05)

    async def _ticker(self) -> None:
        loop = asyncio.get_running_loop()
        nxt = loop.time()
        while True:
            nxt += TICK_S                         # absolute deadlines: no drift, a slow tick does not shift the grid
            try:
                self.seq += 1
                frame = build_frame(self.agg, self.seq, time.time_ns(), self._ic.bridge.mono_to_wall_ns,
                                    self._lag.get_lag(), self.ingest_stats())
                for c in self.clients:
                    if c.queue.full():
                        try:
                            c.queue.get_nowait()          # drop OLDEST
                            c.drops += 1
                        except asyncio.QueueEmpty:
                            pass
                    c.queue.put_nowait(frame)
            except Exception as e:                        # a bad tick must not kill the stream
                self.last_tick_error = f"{type(e).__name__}: {e}"[:200]
            await asyncio.sleep(max(0.0, nxt - loop.time()))
            if loop.time() - nxt > 1.0:                   # stalled >1 s: re-anchor instead of bursting to catch up
                nxt = loop.time()
