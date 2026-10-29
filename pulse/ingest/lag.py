"""Consumer lag of the Fathom processor, via AdminClient (P3-T3, pulled forward to Day 2).

lag = high watermark - committed offset, per partition of the processor's raw-topic subscription. kminion is not used
(it needs a scrape target in Fathom's read-only prometheus.yml, violating S1). Polled every ~2 s on its own thread.
`get_lag()` is a provider callable returning a plain dict, so the frame builder never touches Kafka.
"""
from __future__ import annotations

import threading
import time

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from pulse.ingest.consumer import BOOTSTRAP

TARGET_GROUP = "fathom-processor"
POLL_S = 2.0


class LagMonitor:
    def __init__(self, bootstrap: str = BOOTSTRAP, group: str = TARGET_GROUP) -> None:
        self._group = group
        self._admin = AdminClient({"bootstrap.servers": bootstrap})
        # group-less consumer used only for watermark queries; it never subscribes, so it cannot join any group
        self._probe = Consumer({"bootstrap.servers": bootstrap, "group.id": "pulse-lag-probe", "enable.auto.commit": False})
        self._lock = threading.Lock()
        self._state = {"group": group, "ok": False, "total": None, "per_topic": {}, "polled_at_ms": None, "error": "not polled yet"}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="pulse-lag", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self._probe.close()

    def get_lag(self) -> dict:
        with self._lock:
            d = dict(self._state)
            d["per_topic"] = dict(d["per_topic"])
        d["age_ms"] = None if d["polled_at_ms"] is None else int(time.time() * 1000) - d["polled_at_ms"]
        return d

    def _poll_once(self) -> dict:
        res = self._admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(self._group)])[self._group].result(timeout=5)
        per_topic: dict[str, int] = {}
        for tp in res.topic_partitions:
            if tp.offset is None or tp.offset < 0:      # no committed offset yet: lag undefined, not zero
                continue
            _, high = self._probe.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=5)
            per_topic[tp.topic] = per_topic.get(tp.topic, 0) + max(0, high - tp.offset)
        return {"group": self._group, "ok": True, "total": sum(per_topic.values()) if per_topic else None,
                "per_topic": per_topic, "polled_at_ms": int(time.time() * 1000), "error": None}

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                new = self._poll_once()
            except Exception as e:          # keep the last good numbers' timestamp honest: mark not-ok, keep looping
                with self._lock:
                    self._state.update(ok=False, error=f"{type(e).__name__}: {e}"[:160])
            else:
                with self._lock:
                    self._state = new
            self._stop.wait(POLL_S)
