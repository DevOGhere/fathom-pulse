"""Runtime wiring: ingest -> aggregator -> 10Hz ticker -> per-client queues.

Backpressure (locked): each client has an asyncio.Queue(maxsize=2); when full the OLDEST frame is dropped and the
client's drop counter increments. Drops lose smoothness, never state (frames are self-contained). The counter is
attached to every frame sent to that client and is displayed in the engine lens.
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

from pulse.api.calibration import Calibrator
from pulse.api.frames import build_frame
from pulse.api.incidents import Detector, Obs
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
        self.calibrator = Calibrator(self)
        self.events: list[dict] = []          # server-side event log (self-tests etc.), newest first
        self._inject3_s = 0.0
        self.inj3_actual_ms: list[float] = []
        self._inject_stage: Optional[int] = None
        self._tasks: list[asyncio.Task] = []
        self.detector = Detector()
        self._induced_until = 0.0          # monotonic seconds; incidents overlapping it are labelled induced

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._ic = IngestConsumer(loop, self._queue)
        self._ic.start()
        self._lag.start()
        self._tasks = [asyncio.create_task(self._pump()), asyncio.create_task(self._closer()), asyncio.create_task(self._ticker()), asyncio.create_task(self.calibrator.loop()), asyncio.create_task(self._incident_loop())]

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

    def _release3(self, r: DerivedRec) -> None:
        # ground truth for the stage-3 test: how long after its consume stamp the record REALLY entered the pipeline
        self.inj3_actual_ms.append((time.monotonic_ns() - r.consume_mono_ns) / 1e6)
        self.agg.add_derived(r)

    # ---- calibration hooks ----
    def inject(self, stage: Optional[int], d_ms: float) -> None:
        self._inject_stage = stage
        self._inject3_s = d_ms / 1000 if stage == 3 else 0.0
        if self._ic:
            self._ic.set_inject2(d_ms if stage == 2 else 0.0)

    def injecting(self) -> Optional[dict]:
        return {"stage": self._inject_stage} if self._inject_stage else None

    def consumer_inject_reset(self) -> None:
        self.inj3_actual_ms.clear()
        if self._ic:
            self._ic.inj_actual_ms.clear()

    def consumer_inject_mean(self, stage: int = 2) -> Optional[float]:
        v = list(self.inj3_actual_ms) if stage == 3 else (list(self._ic.inj_actual_ms) if self._ic else [])
        return sum(v) / len(v) if v else None

    def log_event(self, level: str, text: str) -> None:
        self.events.insert(0, {"t_ms": int(time.time() * 1000), "level": level, "text": text})
        del self.events[40:]

    def ingest_stats(self) -> dict:
        ic = self._ic
        return {"queue_drops": ic.queue_drops if ic else 0, "late": self.agg.late,
                "errors": sum(ic.errors.values()) if ic else 0, "clients": len(self.clients),
                "seen": sum(ic.counts.values()) if ic else 0, "decoded": sum(ic.decoded.values()) if ic else 0,
                "poll_alive": bool(ic and ic._thread and ic._thread.is_alive())}

    def induce_incident(self, seconds: float) -> dict:
        """Provoke a real, deliberate feed stall: every record is dropped at ingest for `seconds`. Detector must notice and say 'induced'."""
        seconds = min(max(seconds, 3.0), 30.0)
        self._induced_until = time.monotonic() + seconds + 4.0      # + tail so the recovery close is labelled too
        if self._ic:
            self._ic.drop_for(seconds)
        self.log_event("warning", f"Incident demo: dropping all feed records for {seconds:.0f} s on purpose")
        return {"seconds": seconds, "at_ms": int(time.time() * 1000)}

    async def _incident_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            try:
                now_ns = time.time_ns()
                feeds = {}
                for v, sy in self.agg.keys():
                    la = self.agg.last_consume_wall_ns(v, sy)
                    feeds[f"{v}/{sy}"] = {"rate": len(self.agg.window(v, sy, now_ns - 1_000_000_000)),
                                          "age_s": None if la is None else max(0.0, (now_ns - la) / 1e9)}
                lat = [e[1] + e[2] + e[3] for v, sy in self.agg.keys() for e in self.agg.stage_samples(v, sy, now_ns - 10_000_000_000) if not e[4]]
                lat_ms = sorted(lat)[len(lat) // 2] if len(lat) >= 10 else None
                lag = self._lag.get_lag()
                for kind, inc in self.detector.update(Obs(now_ns // 1_000_000, feeds, lag.get("total") if lag.get("ok") else None,
                                                          lat_ms, time.monotonic() < self._induced_until or self.injecting() is not None)):
                    label = "Incident opened" if kind == "open" else "Incident cleared"
                    self.log_event("warning" if kind == "open" else "good", f"{label}: {inc.kind}{' ' + inc.feed if inc.feed else ''}: {inc.cause}"[:220])
            except Exception as e:
                self.last_tick_error = f"incident loop: {type(e).__name__}: {e}"[:200]

    async def _pump(self) -> None:
        while True:
            loop = asyncio.get_running_loop()
            for r in await self._queue.get():
                if isinstance(r, DerivedRec):
                    if self._inject3_s:           # stage-3 injection: release D after the consume STAMP (not D after now: no compounding)
                        # NOT loop.call_at(): uvicorn[standard] runs uvloop, whose clock base differs from time.monotonic(),
                        # so an absolute time fired immediately and the delay silently vanished (caught by the self-test).
                        due_in = (r.consume_mono_ns + int(self._inject3_s * 1e9) - time.monotonic_ns()) / 1e9
                        loop.call_later(max(0.0, due_in), self._release3, r._replace(induced=True))
                    else:
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
                self.agg.record_stage3(time.monotonic_ns())      # stage 3 (consume -> ready) is known per record; this stamps the tick wait (ready -> THIS frame build)
                frame = build_frame(self.agg, self.seq, time.time_ns(), self._ic.bridge.mono_to_wall_ns,
                                    self._lag.get_lag(), self.ingest_stats(), self.calibrator.snapshot(), self.events[:12], self.detector.snapshot())
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
