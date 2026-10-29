"""Kafka ingest (P1-T1): derived features + raw TRADES for the two live venues.

- Own consumer group `pulse-ingest-<pid>`; never joins or commits as `fathom-processor` (offsets are not committed at all).
  Unique per process on purpose: a shared id makes concurrent instances (e.g. a QA re-run next to the server) SPLIT the
  partitions between them, and each silently sees only part of the stream (found Day 2).
- confluent-kafka is synchronous: a dedicated poll thread decodes and hands batches to an asyncio.Queue.
- Explicit topic list on purpose. A `market.derived.*` regex would also match the stale `binance-replay` and
  `databento.es` topics and create symbols that never tick.
- Raw topics carry snapshots and deltas too: the message-index header is peeked and everything that is not a
  Trade (index 0) is skipped BEFORE ParseFromString. Book reconstruction is out of scope (red-team 5.2).
- Stamps: one monotonic + one wall stamp PER MESSAGE, taken in the unbatching loop (measured in review, 2026-10-29: ~30 ns per call).
  consume() timeout is 5 ms: it blocks for the whole timeout to fill a batch, so the old 50 ms added ~25 ms of Pulse-made
  wait to stage 2. Residual limit: a message that sat in librdkafka's queue before consume() returned it is stamped at
  unbatch time, not at network arrival (<= 5 ms).
- Kafka timestamp = CreateTime = producer enqueue, NOT broker append (cluster is CreateTime; verified Day 0).
- effective_spread == 0.0 is the upstream "no recent trade" sentinel -> None here, at decode time.
"""
from __future__ import annotations

import asyncio
import heapq
import os
import threading
import time
from collections import Counter
from typing import Optional

from confluent_kafka import Consumer, KafkaError

from pulse.ingest import marketdata_pb2 as md
from pulse.ingest import wire
from pulse.ingest.rings import DerivedRec, TradeRec

# Host listener from fathom/docker-compose.yml ("19092:19092 # Kafka API (host)"), same default as fathom's processor.
BOOTSTRAP = "localhost:19092"
GROUP_PREFIX = "pulse-ingest"   # actual group id = prefix + pid: every Pulse instance must see EVERY message
DERIVED_TOPICS = ["market.derived.binance.btcusdt", "market.derived.coinbase.btcusd"]
RAW_TOPICS = ["market.raw.binance.btcusdt", "market.raw.coinbase.btcusd"]
TOPICS = DERIVED_TOPICS + RAW_TOPICS

_IDX_TRADE, _IDX_DERIVED = 0, 4


class ClockBridge:
    """Mono -> wall bridge (S3). Keeps recent (wall, mono) sample pairs; converts using their median offset.

    Monotonic stamps (recv_ts_ns, derived_at_ns) are only comparable to other host-side monotonic stamps. To place
    one on the wall axis, add the offset sampled on this host. Re-sampled periodically so NTP slew is tracked.
    """

    def __init__(self, keep: int = 8) -> None:
        self._offsets: list[int] = []
        self._keep = keep
        self.sample()

    def sample(self) -> None:
        # bracket the wall read between two monotonic reads and use the midpoint to cancel call latency
        m0 = time.monotonic_ns()
        w = time.time_ns()
        m1 = time.monotonic_ns()
        self._offsets.append(w - (m0 + m1) // 2)
        del self._offsets[: -self._keep]

    def mono_to_wall_ns(self, mono_ns: int) -> int:
        o = sorted(self._offsets)[len(self._offsets) // 2]
        return mono_ns + o


class IngestConsumer:
    def __init__(self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue, bootstrap: str = BOOTSTRAP,
                 group: Optional[str] = None) -> None:
        self._loop, self._queue = loop, queue
        self._conf = {
            "bootstrap.servers": bootstrap, "group.id": group or f"{GROUP_PREFIX}-{os.getpid()}",
            "auto.offset.reset": "latest", "enable.auto.commit": False,
            "fetch.wait.max.ms": 50,
        }
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.counts: Counter = Counter()          # per topic, messages seen (incl. skipped book messages)
        self.decoded: Counter = Counter()         # per topic, messages decoded into records
        self.errors: Counter = Counter()
        self.queue_drops = 0
        self.sample_derived: Optional[md.DerivedFeatures] = None
        self.bridge = ClockBridge()
        self._inject2_ns = 0
        self.inj_actual_ms: list[float] = []          # ground-truth injected delay per released message (stage-2 tests)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="pulse-ingest-poll", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    # runs on the event loop
    def _enqueue(self, batch: list) -> None:
        try:
            self._queue.put_nowait(batch)
        except asyncio.QueueFull:
            self.queue_drops += len(batch)

    # ---- calibration hook (stage 2): hold each message and take its consume stamp D later. The stamp is genuinely D
    # later, so the production stage-2 maths sees a real delay. Real data is delayed too; the window is labelled `induced`.
    def set_inject2(self, delay_ms: float) -> None:
        self._inject2_ns = int(delay_ms * 1e6)

    def _run(self) -> None:
        c = Consumer(self._conf)
        c.subscribe(TOPICS)
        last_sample = time.monotonic()
        held: list = []                                   # heap of (due_mono_ns, seq, msg, arrival_mono_ns)
        seq = 0
        try:
            while not self._stop.is_set():
                # consume() blocks for the FULL timeout to fill a batch, so a 50 ms timeout added ~25 ms of Pulse-made wait to stage 2.
                # 5 ms, constant (also while injecting, so a test never changes the natural component it is compared against).
                msgs = c.consume(num_messages=500, timeout=0.005)
                if time.monotonic() - last_sample > 5:
                    self.bridge.sample()
                    last_sample = time.monotonic()
                d = self._inject2_ns
                arrival = time.monotonic_ns()
                batch = []
                for m in msgs:
                    if d and m.topic().startswith("market.derived."):
                        seq += 1
                        heapq.heappush(held, (arrival + d, seq, m, arrival))
                    else:
                        self._handle(m, batch, False)
                now = time.monotonic_ns()
                while held and held[0][0] <= now:
                    _, _, m, arr = heapq.heappop(held)
                    self._handle(m, batch, True, arr)
                if batch:
                    self._loop.call_soon_threadsafe(self._enqueue, batch)
        finally:
            c.close()

    def _handle(self, m, batch: list, induced: bool, arrival_mono_ns: int = 0) -> None:
        mono, wall = time.monotonic_ns(), time.time_ns()     # per-message stamp pair, taken here (review recommendation)
        err = m.error()
        if err:
            if err.code() != KafkaError._PARTITION_EOF:
                self.errors[str(err.code())] += 1
            return
        topic = m.topic()
        self.counts[topic] += 1
        rec = self._decode(topic, m.value(), m.timestamp()[1], mono, wall, induced)
        if rec is not None:
            self.decoded[topic] += 1
            if induced and arrival_mono_ns:
                self.inj_actual_ms.append((mono - arrival_mono_ns) / 1e6)     # ground truth: how long we REALLY held it
            batch.append(rec)

    def _decode(self, topic: str, value: bytes, kafka_ts_ms: int, mono: int, wall: int, induced: bool = False):
        try:
            idx = wire.peek_index(value)
            if topic.startswith("market.derived."):
                if idx != _IDX_DERIVED:
                    return None
                d = wire.decode(value)
                if self.sample_derived is None:
                    self.sample_derived = d
                eff = d.effective_spread if d.effective_spread != 0.0 else None   # sentinel -> None
                return DerivedRec(d.venue, d.symbol, d.derived_at_ns, d.recv_ts_ns, mono, wall, kafka_ts_ms,
                                  d.mid_price, d.microprice, d.obi_top1, d.obi_top5, eff, d.best_bid, d.best_ask, d.best_bid_qty, d.best_ask_qty, induced)
            if idx != _IDX_TRADE:        # book snapshots/deltas: skipped before parsing
                return None
            t = wire.decode(value)
            return TradeRec(t.venue, t.symbol, t.price, t.quantity, mono, wall, kafka_ts_ms, t.is_buyer_maker)
        except wire.WireError:
            self.errors["wire"] += 1
            return None
