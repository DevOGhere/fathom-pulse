"""Ring buffers + 1s closed-bin aggregator (P1-T2).

Pure and deterministic: every method takes the time as an argument, nothing reads a clock. That makes the
closed-bin rule (S7) testable without a live feed.

Rules
- Bin assignment time = PULSE'S OWN consume wall time (time.time_ns() stamped when the poll thread got the
  message), never the exchange time. One clock, one place: that is what makes bins comparable across venues.
- Bin [s, s+1) closes when now >= (s+1)s + BIN_GRACE_NS. The grace lets in-flight messages land. A message that
  arrives for an already-closed second is counted in `late` and dropped; a closed bin is never rewritten.
- Cadence is exact: once a key has started, EVERY second produces exactly one bin. Seconds with no messages
  produce an explicit empty bin (n_derived = 0, KPIs None), so a stall is visible, not a gap in the chart.
- effective_spread arrives as None for the upstream 0.0 "no recent trade" sentinel (mapped at decode time).
  It is never averaged or zero-filled; the bin carries the last non-null value seen in that second.
- realized_vol_60s = sqrt(sum of squared 1s log returns of bin-close mids) over the trailing 60 bins, using only
  non-empty bins, None with fewer than 2 returns. Per-60s, NOT annualised.
- avg_trade_size_60s = total traded quantity / trade count over the trailing 60 bins, None when no trades.
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import asdict, dataclass
from typing import NamedTuple, Optional

NS = 1_000_000_000
BIN_GRACE_NS = 250_000_000
BIN_RING_CAP = 300       # 5 minutes of closed 1s bins per key
RAW_RING_CAP = 20_000    # recent derived records per key (~47/s total, so well over 5 min)
VOL_WINDOW = 60
STAGE_RING_CAP = 6000    # per-record stage samples kept per key (~5 min at 20/s)
TRADE_TAIL_CAP = 50      # recent trades kept per key for the time-and-sales tail


class DerivedRec(NamedTuple):
    venue: str
    symbol: str
    derived_at_ns: int        # processor monotonic stamp (proto field)
    recv_ts_ns: int           # producer monotonic stamp (proto field)
    consume_mono_ns: int      # Pulse monotonic stamp when the batch was returned by Kafka
    consume_wall_ns: int      # Pulse wall stamp at the same moment (bin assignment uses this)
    kafka_ts_ms: int          # Kafka CreateTime = producer enqueue (wall); NOT broker append
    mid: float
    microprice: float
    obi_top1: float
    obi_top5: float
    effective_spread: Optional[float]   # None = sentinel / no recent trade
    best_bid: float
    best_ask: float
    best_bid_qty: float = 0.0
    best_ask_qty: float = 0.0
    induced: bool = False        # True while a calibration delay is being injected at a Pulse stamp site


class TradeRec(NamedTuple):
    venue: str
    symbol: str
    price: float
    quantity: float
    consume_mono_ns: int
    consume_wall_ns: int
    kafka_ts_ms: int
    is_buyer_maker: bool = False   # True => the BUYER was the maker => the AGGRESSOR was the seller (same on both venues)


@dataclass
class Bin:
    venue: str
    symbol: str
    start_s: int                      # bin covers [start_s, start_s + 1) in Pulse wall seconds
    close_ts_ms: int                  # (start_s + 1) * 1000
    n_derived: int                    # = quote_rate_1s
    mid: Optional[float]
    microprice: Optional[float]
    obi_top1: Optional[float]
    obi_top5: Optional[float]
    spread_abs: Optional[float]
    effective_spread: Optional[float]
    trade_count_1s: int
    avg_trade_size_60s: Optional[float]
    realized_vol_60s: Optional[float]
    stages: Optional[dict] = None      # per-bin stage p50/p99 (ms): s1 recv->derived, s2 derived->consume, s3 consume->ready for a frame, tw tick wait (ready->frame build), tot (= s1+s2+s3)
    induced: bool = False              # a calibration delay was injected during this second; excluded from SLO/histograms

    @property
    def empty(self) -> bool:
        return self.n_derived == 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["bin"] = "1s"
        d["empty"] = self.empty
        return d


class _Accum:
    __slots__ = ("n", "mid", "micro", "obi1", "obi5", "bid", "ask", "eff", "n_trades", "trade_qty", "s1", "s2", "s3", "tw", "tot", "induced", "neg")

    def __init__(self) -> None:
        self.n = 0
        self.mid = self.micro = self.obi1 = self.obi5 = self.bid = self.ask = self.eff = None
        self.n_trades = 0
        self.trade_qty = 0.0
        self.s1: list[float] = []
        self.s2: list[float] = []
        self.s3: list[float] = []
        self.tw: list[float] = []
        self.tot: list[float] = []
        self.induced = False
        self.neg = 0


class _Key:
    __slots__ = ("next_sec", "open", "bins", "raw", "win", "trades", "stages", "pending")

    def __init__(self, bin_cap: int, raw_cap: int) -> None:
        self.next_sec: Optional[int] = None
        self.open: dict[int, _Accum] = {}
        self.bins: deque[Bin] = deque(maxlen=bin_cap)
        self.raw: deque[DerivedRec] = deque(maxlen=raw_cap)
        # trailing window of (close_mid_or_None, trade_count, trade_qty) per closed bin
        self.win: deque[tuple] = deque(maxlen=VOL_WINDOW)
        self.trades: deque[TradeRec] = deque(maxlen=TRADE_TAIL_CAP)
        self.stages: deque[list] = deque(maxlen=STAGE_RING_CAP)   # [consume_wall_ns, s1, s2, s3 (consume->ready)|None, induced, tick_wait|None]
        self.pending: list = []                                    # (entry, consume_mono_ns, second, ready_mono_ns) awaiting frame-build stamp


class Aggregator:
    def __init__(self, bin_cap: int = BIN_RING_CAP, raw_cap: int = RAW_RING_CAP, grace_ns: int = BIN_GRACE_NS) -> None:
        self._bin_cap, self._raw_cap, self._grace = bin_cap, raw_cap, grace_ns
        self._keys: dict[tuple[str, str], _Key] = {}
        self.late = 0

    def _key(self, venue: str, symbol: str) -> _Key:
        k = self._keys.get((venue, symbol))
        if k is None:
            k = self._keys[(venue, symbol)] = _Key(self._bin_cap, self._raw_cap)
        return k

    def _accum(self, k: _Key, wall_ns: int) -> Optional[_Accum]:
        sec = wall_ns // NS
        if k.next_sec is None:
            k.next_sec = sec
        if sec < k.next_sec:        # that second is already closed: never rewrite a closed bin
            self.late += 1
            return None
        return k.open.setdefault(sec, _Accum())

    def add_derived(self, r: DerivedRec) -> bool:
        k = self._key(r.venue, r.symbol)
        a = self._accum(k, r.consume_wall_ns)
        if a is None:
            return False
        a.n += 1
        a.mid, a.micro, a.obi1, a.obi5 = r.mid, r.microprice, r.obi_top1, r.obi_top5
        a.bid, a.ask = r.best_bid, r.best_ask
        if r.effective_spread is not None:      # never fold the sentinel into anything
            a.eff = r.effective_spread
        k.raw.append(r)
        s1 = (r.derived_at_ns - r.recv_ts_ns) / 1e6        # producer recv -> processor derived   (both host monotonic)
        s2 = (r.consume_mono_ns - r.derived_at_ns) / 1e6   # processor derived -> Pulse consumed  (both host monotonic)
        a.s1.append(s1)
        a.s2.append(s2)
        a.neg += (s1 < 0) + (s2 < 0)
        a.induced = a.induced or r.induced
        entry = [r.consume_wall_ns, s1, s2, None, r.induced, None]
        k.stages.append(entry)
        # ready = in the aggregator, waiting for the next scheduled frame build. ready -> build is tick wait (design, <= 100 ms)
        k.pending.append((entry, r.consume_mono_ns, r.consume_wall_ns // NS, time.monotonic_ns()))
        return True

    def record_stage3(self, build_mono_ns: int) -> None:
        """Stage 3 = consume -> ready (in the aggregator). Tick wait = ready -> the 10Hz frame build that first includes the record. Called by the ticker right before it builds."""
        for k in self._keys.values():
            for entry, cmono, sec, ready in k.pending:
                s3 = (ready - cmono) / 1e6                    # stage 3 = consume -> ready in the aggregator (exact: no tick quantisation)
                tw = (build_mono_ns - ready) / 1e6            # tick wait: ready -> the scheduled frame build (cadence, not processing)
                entry[3], entry[5] = s3, tw
                a = k.open.get(sec)
                if a is not None:
                    a.s3.append(s3)
                    a.tw.append(tw)
                    a.tot.append(entry[1] + entry[2] + s3)
                    a.neg += s3 < 0
            k.pending.clear()

    def stage_samples(self, venue: str, symbol: str, since_wall_ns: int, until_wall_ns: Optional[int] = None) -> list[list]:
        """Complete per-record samples (stage 3 known), oldest first. Entries: [wall_ns, s1, s2, s3, induced]."""
        k = self._keys.get((venue, symbol))
        if not k:
            return []
        return [e for e in k.stages if e[3] is not None and e[0] >= since_wall_ns and (until_wall_ns is None or e[0] < until_wall_ns)]

    def add_trade(self, r: TradeRec) -> bool:
        k = self._key(r.venue, r.symbol)
        a = self._accum(k, r.consume_wall_ns)
        if a is None:
            return False
        a.n_trades += 1
        a.trade_qty += r.quantity
        k.trades.append(r)
        return True

    def close_due(self, now_ns: int) -> list[Bin]:
        """Close every second whose deadline has passed. Returns the newly closed bins (per key, in order)."""
        out: list[Bin] = []
        for (venue, symbol), k in self._keys.items():
            while k.next_sec is not None and now_ns >= (k.next_sec + 1) * NS + self._grace:
                sec = k.next_sec
                a = k.open.pop(sec, None)
                close_mid = a.mid if a is not None else None
                k.win.append((close_mid, a.n_trades if a else 0, a.trade_qty if a else 0.0))
                b = Bin(
                    venue=venue, symbol=symbol, start_s=sec, close_ts_ms=(sec + 1) * 1000,
                    n_derived=a.n if a else 0,
                    mid=close_mid, microprice=a.micro if a else None,
                    obi_top1=a.obi1 if a else None, obi_top5=a.obi5 if a else None,
                    spread_abs=(a.ask - a.bid) if (a and a.ask is not None and a.bid is not None) else None,
                    effective_spread=a.eff if a else None,
                    trade_count_1s=a.n_trades if a else 0,
                    avg_trade_size_60s=self._avg_trade(k), realized_vol_60s=self._vol(k),
                    stages=self._stage_stats(a), induced=bool(a and a.induced),
                )
                k.bins.append(b)
                out.append(b)
                k.next_sec = sec + 1
        return out

    @staticmethod
    def _stage_stats(a: Optional[_Accum]) -> Optional[dict]:
        if a is None or not a.s1:
            return None
        def st(v):
            if not v:
                return {"p50": None, "p99": None}
            q = sorted(v)
            return {"p50": round(q[len(q) // 2], 3), "p99": round(q[min(len(q) - 1, int(0.99 * len(q)))], 3)}
        return {"s1": st(a.s1), "s2": st(a.s2), "s3": st(a.s3), "tw": st(a.tw), "tot": st(a.tot), "neg": a.neg, "n": len(a.s1)}

    @staticmethod
    def _vol(k: _Key) -> Optional[float]:
        mids = [m for m, _, _ in k.win if m is not None and m > 0]
        rets = [math.log(b / a) for a, b in zip(mids, mids[1:])]
        if len(rets) < 2:
            return None
        return math.sqrt(sum(r * r for r in rets))

    @staticmethod
    def _avg_trade(k: _Key) -> Optional[float]:
        n = sum(c for _, c, _ in k.win)
        return (sum(q for _, _, q in k.win) / n) if n else None

    # ---- read side ----
    def keys(self) -> list[tuple[str, str]]:
        return list(self._keys)

    def bins(self, venue: str, symbol: str) -> list[Bin]:
        k = self._keys.get((venue, symbol))
        return list(k.bins) if k else []

    def window(self, venue: str, symbol: str, since_wall_ns: int) -> list[DerivedRec]:
        """Derived records consumed at or after `since_wall_ns` (oldest first). Used for the rolling-1s snapshot."""
        k = self._keys.get((venue, symbol))
        if not k:
            return []
        out: list[DerivedRec] = []
        for r in reversed(k.raw):
            if r.consume_wall_ns < since_wall_ns:
                break
            out.append(r)
        out.reverse()
        return out

    def recent_trades(self, venue: str, symbol: str, n: int) -> list[TradeRec]:
        k = self._keys.get((venue, symbol))
        return list(k.trades)[-n:] if k else []

    def recent_bins(self, venue: str, symbol: str, n: int) -> list[Bin]:
        k = self._keys.get((venue, symbol))
        return list(k.bins)[-n:] if k else []

    def assert_caps(self) -> None:
        """Memory bound (P1-T2 spec): no ring may exceed its configured cap."""
        for key, k in self._keys.items():
            assert len(k.bins) <= self._bin_cap, f"bin ring over cap for {key}"
            assert len(k.raw) <= self._raw_cap, f"raw ring over cap for {key}"
            assert len(k.open) <= 10, f"open-bin backlog growing for {key}: {len(k.open)}"
