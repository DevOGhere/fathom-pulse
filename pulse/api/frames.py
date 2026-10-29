"""Frame assembly (S4): the ONE module that turns Pulse state into a wire frame. Protobuf can swap in here at v1.1.

Frame semantics (LOCKED 2026-07-03): a self-contained snapshot of the trailing 1-second window, emitted at 10Hz.
No deltas. A client that missed any number of frames renders correctly from any single frame. Frames are built once
per tick and shared; the per-client drop counter is attached at send time (hub.py), not here.

Clock rules (S3): `*_age_ms` values are wall-clock differences; `stage_*` values are differences of two host-side
MONOTONIC stamps. They are never mixed. A negative stage duration means the clock-domain assumption is broken, so
it is counted (`neg_stage`) instead of being clamped away. Kafka CreateTime is "producer enqueue", not broker append.
"""
from __future__ import annotations

from typing import Optional

from pulse.ingest.rings import NS, Aggregator

SCHEMA_VERSION = 1
HIST_BINS = 60


def _pct(vals: list[float], p: float) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    return round(s[min(len(s) - 1, int(p / 100 * len(s)))], 3)


def _stage(vals: list[float]) -> dict:
    return {"p50": _pct(vals, 50), "p99": _pct(vals, 99)}


def build_frame(agg: Aggregator, seq: int, now_wall_ns: int, mono_to_wall, lag: dict, ingest: dict) -> dict:
    """`mono_to_wall` is ClockBridge.mono_to_wall_ns (injected so this stays a pure function)."""
    series: dict = {}
    newest_consume = None
    for venue, symbol in sorted(agg.keys()):
        win = agg.window(venue, symbol, now_wall_ns - NS)
        bins = agg.recent_bins(venue, symbol, HIST_BINS)
        entry: dict = {"window_1s": None, "bins": None}
        if win:
            last = win[-1]
            newest_consume = last.consume_wall_ns if newest_consume is None else max(newest_consume, last.consume_wall_ns)
            s1 = [(r.derived_at_ns - r.recv_ts_ns) / 1e6 for r in win]          # producer recv -> processor derived
            s2 = [(r.consume_mono_ns - r.derived_at_ns) / 1e6 for r in win]     # processor derived -> Pulse consumed
            entry["window_1s"] = {
                "n": len(win),
                "mid": last.mid, "microprice": last.microprice,
                "obi_top1": last.obi_top1, "obi_top5": last.obi_top5,
                "spread_abs": last.best_ask - last.best_bid,
                "effective_spread": next((r.effective_spread for r in reversed(win) if r.effective_spread is not None), None),
                "derived_age_ms": round((now_wall_ns - mono_to_wall(last.derived_at_ns)) / 1e6, 2),
                "pulse_consume_age_ms": round((now_wall_ns - last.consume_wall_ns) / 1e6, 2),
                "kafka_age_ms": now_wall_ns // 1_000_000 - last.kafka_ts_ms,    # label: producer enqueue
                "stage_recv_to_derived_ms": _stage(s1),
                "stage_derived_to_consume_ms": _stage(s2),
                "neg_stage": sum(1 for v in s1 if v < 0) + sum(1 for v in s2 if v < 0),
            }
        if bins:
            entry["bins"] = {
                "start_s": [b.start_s for b in bins],
                "mid": [b.mid for b in bins],
                "microprice": [b.microprice for b in bins],
                "obi_top1": [b.obi_top1 for b in bins],
                "n_derived": [b.n_derived for b in bins],
                "trade_count_1s": [b.trade_count_1s for b in bins],
                "realized_vol_60s": bins[-1].realized_vol_60s,
                "avg_trade_size_60s": bins[-1].avg_trade_size_60s,
            }
        series[f"{venue}/{symbol}"] = entry
    return {
        "v": SCHEMA_VERSION, "seq": seq, "ts_ms": now_wall_ns // 1_000_000,
        "pulse_age_ms": None if newest_consume is None else round((now_wall_ns - newest_consume) / 1e6, 2),
        "lag": lag, "ingest": ingest, "series": series,
    }
