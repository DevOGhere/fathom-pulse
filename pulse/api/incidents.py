"""Incident detection (pure, no I/O): state machines over once-a-second observations, with hysteresis and a stated cause.

Three rules per feed, all on measured quantities:
- stall      : no derived record consumed for > STALL_OPEN_S (closes once the last-message age is back under STALL_CLOSE_S).
- rate_drop  : the 5 s mean update rate is a robust outlier BELOW the preceding 60 s (median/MAD z < -Z_RATE and < RATE_RATIO of
               the median), only judged when the baseline is healthy (median >= MIN_BASE_RATE) and the feed is not stalled.
- lag_spike  : processor lag (messages behind) above max(LAG_FLOOR, median + Z_LAG robust sigmas) for >= LAG_SECONDS in a row.
The cause text states only what the evidence supports: it never names a culprit it cannot see. An incident overlapping a deliberate
test (`induced=True`) is labelled as induced and never reported as a real outage.
"""
from __future__ import annotations

import statistics
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

STALL_OPEN_S, STALL_CLOSE_S = 3.0, 1.0
Z_RATE, RATE_RATIO, MIN_BASE_RATE, BASE_S, FAST_S = 4.0, 0.5, 3.0, 60, 5
RATE_PERSIST = 3      # the rate_drop condition must hold this many seconds in a row (a 1 s hiccup on both feeds fired it at z=-4.1 on 2026-11-01)
LAG_FLOOR, Z_LAG, LAG_SECONDS = 200, 5.0, 3
DRIFT_MIN_MS, Z_DRIFT, DRIFT_RATIO = 20.0, 5.0, 1.5     # latency_drift: needs >= +20 ms AND > 1.5x the preceding minute AND > 5 robust sigma
KEEP = 40


@dataclass
class Obs:
    t_ms: int
    feeds: dict          # "venue/symbol" -> {"rate": updates in the last 1 s (int), "age_s": seconds since last record ever, or None}
    lag: Optional[int]   # processor lag in messages, None if unavailable
    lat_ms: Optional[float] = None    # median per-record pipeline latency over the last 10 s, calibration-induced records excluded; None if too few records
    induced: bool = False


@dataclass
class Incident:
    id: int
    kind: str
    feed: Optional[str]
    start_ms: int
    severity: str
    cause: str
    evidence: dict
    induced: bool
    end_ms: Optional[int] = None

    def to_dict(self) -> dict:
        return {"id": self.id, "kind": self.kind, "feed": self.feed, "start_ms": self.start_ms, "end_ms": self.end_ms,
                "severity": self.severity, "cause": self.cause, "evidence": self.evidence, "induced": self.induced}


def _robust(vals: list[float]) -> tuple[float, float]:
    med = statistics.median(vals)
    mad = statistics.median([abs(v - med) for v in vals])
    return med, max(1.4826 * mad, 0.5)          # floor: a perfectly flat baseline must not make any wobble look infinite


class Detector:
    def __init__(self) -> None:
        self.rates: dict[str, deque] = {}
        self.lags: deque = deque(maxlen=BASE_S)
        self.open: dict[tuple, Incident] = {}
        self.done: deque = deque(maxlen=KEEP)
        self._n = 0
        self._lag_run = 0
        self.lats: deque = deque(maxlen=BASE_S + 5)
        self._rate_run: dict[str, int] = {}
        self._cool: dict[str, int] = {}      # seconds left after a stall during which rate_drop is not judged (its zeros are the stall's, not a second incident)

    # ---- helpers ----
    def _start(self, kind, feed, o: Obs, severity, cause, evidence) -> None:
        self._n += 1
        self.open[(kind, feed)] = Incident(self._n, kind, feed, o.t_ms, severity, cause, evidence, o.induced)

    def _end(self, key, o: Obs) -> None:
        inc = self.open.pop(key)
        inc.end_ms = o.t_ms
        self.done.append(inc)

    def _cause_silent(self, feed: str, o: Obs) -> str:
        silent = [k for k, f in o.feeds.items() if f["age_s"] is not None and f["age_s"] > STALL_OPEN_S]
        lag_ok = o.lag is not None and o.lag < LAG_FLOOR
        where = "both feeds went quiet together" if len(silent) > 1 else f"only {feed} went quiet; the other feed kept flowing"
        if o.induced:
            return "Induced: Pulse was told to drop this feed's records on purpose (incident demo)."
        if lag_ok:
            return (f"Records stopped reaching Pulse, and the processor has no backlog ({o.lag} msgs behind), so nothing is queued up: "
                    f"the gap is upstream of Fathom's processor (exchange feed, network or producer). {where}.")
        return f"Records stopped, and the processor is {o.lag if o.lag is not None else 'unknown'} msgs behind, so the cause is not clear from Pulse's side. {where}."

    # ---- main entry: call once per second ----
    def update(self, o: Obs) -> list[tuple[str, Incident]]:
        before = {k: v.id for k, v in self.open.items()}
        for feed, f in o.feeds.items():
            hist = self.rates.setdefault(feed, deque(maxlen=BASE_S + FAST_S))
            age = f["age_s"]
            # stall
            key = ("stall", feed)
            if key not in self.open and age is not None and age > STALL_OPEN_S:
                self._start("stall", feed, o, "serious", self._cause_silent(feed, o), {"age_s": round(age, 1), "lag": o.lag})
            elif key in self.open:
                inc = self.open[key]
                if age is not None and age > 10 and inc.severity != "critical":
                    inc.severity = "critical"
                inc.evidence["peak_age_s"] = round(max(inc.evidence.get("peak_age_s", 0), age or 0), 1)
                if age is not None and age < STALL_CLOSE_S:
                    self._end(key, o)
            stalled = ("stall", feed) in self.open
            if stalled:
                self._cool[feed] = FAST_S + 1
            elif self._cool.get(feed, 0) > 0:
                self._cool[feed] -= 1
            cooling = self._cool.get(feed, 0) > 0
            # rate drop (judged against history from BEFORE this second, never while stalled: that is the stall's job)
            base = list(hist)[:-FAST_S] if len(hist) > FAST_S else []
            fast = list(hist)[-FAST_S:] + [f["rate"]]
            fast = fast[-FAST_S:]
            key = ("rate_drop", feed)
            if len(base) >= 30 and not stalled and not cooling:
                med, sig = _robust(base)
                mean_fast = sum(fast) / len(fast)
                z = (mean_fast - med) / sig
                hit = med >= MIN_BASE_RATE and z < -Z_RATE and mean_fast < RATE_RATIO * med
                self._rate_run[feed] = self._rate_run.get(feed, 0) + 1 if hit else 0
                if key not in self.open and self._rate_run[feed] >= RATE_PERSIST:
                    self._start("rate_drop", feed, o, "warning",
                                ("Induced: Pulse was told to drop records on purpose (incident demo)." if o.induced else
                                 f"Update rate fell to {mean_fast:.1f}/s from a normal {med:.1f}/s while the feed is still delivering. " +
                                 ("Processor has no backlog, so the slowdown is upstream of it." if (o.lag is not None and o.lag < LAG_FLOOR) else "Processor is behind, so it may be the cause.")),
                                {"rate_now": round(mean_fast, 2), "rate_normal": round(med, 2), "z": round(z, 1)})
                elif key in self.open and mean_fast >= 0.7 * med:
                    self._end(key, o)
            elif key in self.open and not stalled and f["rate"] >= MIN_BASE_RATE:
                self._end(key, o)
            hist.append(f["rate"])
        # lag spike
        if o.lag is not None:
            base = list(self.lags)
            thr = LAG_FLOOR
            if len(base) >= 30:
                med, sig = _robust(base)
                thr = max(LAG_FLOOR, med + Z_LAG * sig)
            self._lag_run = self._lag_run + 1 if o.lag > thr else 0
            key = ("lag_spike", None)
            if key not in self.open and self._lag_run >= LAG_SECONDS:
                self._start("lag_spike", None, o, "serious", "Fathom's processor is falling behind: messages are queueing between the broker and the processor, so every number downstream gets older. Pulse is reading the same stream and sees the delay too.", {"lag": o.lag, "threshold": round(thr)})
            elif key in self.open:
                self.open[key].evidence["peak_lag"] = max(self.open[key].evidence.get("peak_lag", 0), o.lag)
                if self._lag_run == 0 and o.lag < thr * 0.5:
                    self._end(key, o)
            self.lags.append(o.lag)
        # latency drift: the pipeline got slower while records still flow
        if o.lat_ms is not None:
            self.lats.append(o.lat_ms)
            base, fast = list(self.lats)[:-5], list(self.lats)[-5:]
            key = ("latency_drift", None)
            if len(base) >= 30 and len(fast) == 5:
                med, sig = _robust(base)
                mean_fast = sum(fast) / 5
                rise = mean_fast - med
                if key not in self.open and rise >= max(DRIFT_MIN_MS, Z_DRIFT * sig) and mean_fast > DRIFT_RATIO * med:
                    self._start("latency_drift", None, o, "warning",
                                ("Induced: Pulse was told to slow its own pipeline on purpose (incident demo)." if o.induced else
                                 f"Per-record pipeline latency rose to {mean_fast:.0f} ms from a normal {med:.0f} ms while records keep flowing. "
                                 + ("The processor has no backlog, so look at the stages in the Anatomy panel: Pulse can show which stage grew." if (o.lag is not None and o.lag < LAG_FLOOR) else "The processor is behind, which would explain it.")),
                                {"latency_now_ms": round(mean_fast, 1), "latency_normal_ms": round(med, 1)})
                elif key in self.open:
                    ev = self.open[key].evidence
                    ev["peak_ms"] = round(max(ev.get("peak_ms", 0), mean_fast), 1)
                    if rise < max(DRIFT_MIN_MS / 2, 2.5 * sig):
                        self._end(key, o)
        return [("open", v) for k, v in self.open.items() if before.get(k) != v.id] + \
               [("close", v) for v in list(self.done)[-3:] if v.end_ms == o.t_ms]

    def snapshot(self) -> list[dict]:
        return [i.to_dict() for i in list(self.done)[-12:]] + [i.to_dict() for i in self.open.values()]
