"""Kafka ingest (P1-T1): derived features + raw TRADES for the two live venues.

- Own consumer group `pulse-ingest-<pid>`; never joins or commits as `fathom-processor` (offsets are not committed at all).
  Unique per process on purpose: a shared id makes concurrent instances (e.g. a QA re-run next to the server) SPLIT the
  partitions between them, and each silently sees only part of the stream (found Day 2).
- confluent-kafka is synchronous: a dedicated poll thread decodes and hands batches to an asyncio.Queue.
- Explicit topic list on purpose. A `market.derived.*` regex would also match the stale `binance-replay` and
  `databento.es` topics and create symbols that never tick.
- Raw topics carry snapshots and deltas too: the message-index header is peeked and everything that is not a
  Trade (index 0) is skipped BEFORE ParseFromString. Book reconstruction is out of scope (red-team 5.2).
- Stamps: one monotonic + one wall stamp per poll batch, taken right after Kafka returns it. A message's
  `consume` time therefore has batch resolution (<= the 50 ms consume timeout). That is stated, not hidden.
- Kafka timestamp = CreateTime = producer enqueue, NOT broker append (cluster is CreateTime; verified Day 0).
- effective_spread == 0.0 is the upstream "no recent trade" sentinel -> None here, at decode time.
"""
from __future__ import annotations

import asyncio
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

    def _run(self) -> None:
        c = Consumer(self._conf)
        c.subscribe(TOPICS)
        last_sample = time.monotonic()
        try:
            while not self._stop.is_set():
                msgs = c.consume(num_messages=500, timeout=0.05)
                if time.monotonic() - last_sample > 5:
                    self.bridge.sample()
                    last_sample = time.monotonic()
                if not msgs:
                    continue
                mono, wall = time.monotonic_ns(), time.time_ns()     # one stamp pair for the whole batch
                batch = []
                for m in msgs:
                    err = m.error()
                    if err:
                        if err.code() != KafkaError._PARTITION_EOF:
                            self.errors[str(err.code())] += 1
                        continue
                    topic = m.topic()
                    self.counts[topic] += 1
                    rec = self._decode(topic, m.value(), m.timestamp()[1], mono, wall)
                    if rec is not None:
                        self.decoded[topic] += 1
                        batch.append(rec)
                if batch:
                    self._loop.call_soon_threadsafe(self._enqueue, batch)
        finally:
            c.close()

    def _decode(self, topic: str, value: bytes, kafka_ts_ms: int, mono: int, wall: int):
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
                                  d.mid_price, d.microprice, d.obi_top1, d.obi_top5, eff, d.best_bid, d.best_ask)
            if idx != _IDX_TRADE:        # book snapshots/deltas: skipped before parsing
                return None
            t = wire.decode(value)
            return TradeRec(t.venue, t.symbol, t.price, t.quantity, mono, wall, kafka_ts_ms)
        except wire.WireError:
            self.errors["wire"] += 1
            return None
