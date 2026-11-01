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
TRADES_IN_FRAME = 12
HIST_EDGES_MS = (0, 5, 10, 15, 20, 30, 40, 60, 80, 100, 150, 200)    # bucket i = [edges[i], edges[i+1]); the last is open-ended
SLO_TARGET_MS = 100.0     # p99 of per-record PIPELINE total (recv -> ready for a frame, tick wait excluded) over 60 s


def _pct(vals: list[float], p: float) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    return round(s[min(len(s) - 1, int(p / 100 * len(s)))], 3)


def _stage(vals: list[float]) -> dict:
    return {"p50": _pct(vals, 50), "p99": _pct(vals, 99)}


def build_frame(agg: Aggregator, seq: int, now_wall_ns: int, mono_to_wall, lag: dict, ingest: dict,
                calibration: Optional[dict] = None, events: Optional[list] = None, incidents: Optional[list] = None) -> dict:
    """`mono_to_wall` is ClockBridge.mono_to_wall_ns (injected so this stays a pure function)."""
    series: dict = {}
    newest_consume = None
    for venue, symbol in sorted(agg.keys()):
        win = agg.window(venue, symbol, now_wall_ns - NS)
        bins = agg.recent_bins(venue, symbol, HIST_BINS)
        entry: dict = {"window_1s": None, "bins": None, "trades": []}
        lastc = agg.last_consume_wall_ns(venue, symbol)       # newest record EVER: an outage must read as growing age, never as "no data yet"
        entry["last_msg_age_ms"] = None if lastc is None else round(max(0.0, (now_wall_ns - lastc) / 1e6), 1)
        if lastc is not None:
            newest_consume = lastc if newest_consume is None else max(newest_consume, lastc)
        if win:
            last = win[-1]
            s1 = [(r.derived_at_ns - r.recv_ts_ns) / 1e6 for r in win]          # producer recv -> processor derived
            s2 = [(r.consume_mono_ns - r.derived_at_ns) / 1e6 for r in win]     # processor derived -> Pulse consumed
            entry["window_1s"] = {
                "n": len(win),
                "mid": last.mid, "microprice": last.microprice,
                "obi_top1": last.obi_top1, "obi_top5": last.obi_top5,
                "spread_abs": last.best_ask - last.best_bid,
                "best_bid": last.best_bid, "best_ask": last.best_ask,
                "best_bid_qty": last.best_bid_qty, "best_ask_qty": last.best_ask_qty,
                "effective_spread": next((r.effective_spread for r in reversed(win) if r.effective_spread is not None), None),
                "derived_age_ms": round((now_wall_ns - mono_to_wall(last.derived_at_ns)) / 1e6, 2),
                "pulse_consume_age_ms": round((now_wall_ns - last.consume_wall_ns) / 1e6, 2),
                "kafka_age_ms": now_wall_ns // 1_000_000 - last.kafka_ts_ms,    # label: producer enqueue
                "stage_recv_to_derived_ms": _stage(s1),
                "stage_derived_to_consume_ms": _stage(s2),
                "neg_stage": sum(1 for v in s1 if v < 0) + sum(1 for v in s2 if v < 0),
            }
        # aggressor = seller when is_buyer_maker (verified against producer/venues.py on both venues)
        entry["trades"] = [{"t_ms": t.consume_wall_ns // 1_000_000, "px": t.price, "qty": t.quantity,
                            "aggressor": "sell" if t.is_buyer_maker else "buy"} for t in agg.recent_trades(venue, symbol, TRADES_IN_FRAME)]
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
                "stages": {k2: [None if b.stages is None else b.stages[k1][pp] for b in bins]
                           for k1, k2, pp in [("s1", "s1_p50", "p50"), ("s1", "s1_p99", "p99"), ("s2", "s2_p50", "p50"), ("s2", "s2_p99", "p99"),
                                              ("s3", "s3_p50", "p50"), ("s3", "s3_p99", "p99"), ("tw", "tw_p50", "p50"), ("tw", "tw_p99", "p99"), ("tot", "tot_p50", "p50"), ("tot", "tot_p99", "p99")]},
                "induced": [b.induced for b in bins],
            }
            ne = [b for b in bins if not b.empty]
            entry["dq"] = {"empty_bins_60": sum(1 for b in bins if b.empty), "bins_60": len(bins),
                           "sentinel_rate_60": None if not ne else round(sum(1 for b in ne if b.effective_spread is None) / len(ne), 3),
                           "stall_s": None if not win and not bins else round(max(0.0, (now_wall_ns - (win[-1].consume_wall_ns if win else (bins[-1].close_ts_ms * 1_000_000))) / 1e9), 2)}
        series[f"{venue}/{symbol}"] = entry
    # SLO from per-RECORD totals, not a sum of per-stage p99s; induced seconds excluded. Pipeline = recv -> ready for a frame:
    # the scheduled 10 Hz tick wait (<= 100 ms, a display-cadence choice) is reported beside it, never hidden inside it.
    live = [e for v, sy in agg.keys() for e in agg.stage_samples(v, sy, now_wall_ns - 60 * NS) if not e[4] and e[5] is not None]
    tots = [e[1] + e[2] + e[3] for e in live]
    p99 = _pct(tots, 99)
    counts = [0] * len(HIST_EDGES_MS)
    for v in tots:
        counts[max(0, sum(1 for e in HIST_EDGES_MS[1:] if v >= e))] += 1
    slo = {"target_ms": SLO_TARGET_MS, "window_s": 60, "n": len(tots), "p99_ms": p99,
           "hist": {"edges_ms": list(HIST_EDGES_MS), "counts": counts},
           "met": None if p99 is None else p99 < SLO_TARGET_MS,
           "tick_wait_p50_ms": _pct([e[5] for e in live], 50), "tick_wait_p99_ms": _pct([e[5] for e in live], 99),
           "end_to_frame_p99_ms": _pct([e[1] + e[2] + e[3] + e[5] for e in live], 99)}
    return {
        "v": SCHEMA_VERSION, "seq": seq, "ts_ms": now_wall_ns // 1_000_000,
        "pulse_age_ms": None if newest_consume is None else round((now_wall_ns - newest_consume) / 1e6, 2),
        "lag": lag, "ingest": ingest, "series": series, "slo": slo,
        "calibration": calibration, "events": events or [], "incidents": incidents or [], "induced": bool(calibration and calibration.get("injecting")),
    }
