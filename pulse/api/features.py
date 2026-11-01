"""pulse.fv.1: the feature-vector contract (pure, no I/O). Served by GET /features/latest and /features/window.

- One vector = one CLOSED 1 s bin, never a partial one (train/serve parity). Latest and window return the identical shape.
- Additive-only within fv.1: new fields may be appended, existing ones never change meaning or type. Breaking = fv.2 at /v2/.
  tests/test_features.py freezes the field list; extending it is a deliberate edit of that golden list.
- Honest nulls: empty seconds and the effective_spread sentinel are null, never 0. `empty` and `induced` ride along so a
  consumer can drop empty seconds and calibration-induced seconds from training data.
- provenance: derived_lag_ms_p50 = median per-record pipeline latency (recv -> derived -> consumed -> ready, ms) over that
  second; pulse_age_ms = how old the vector is at request time (now - bin close, wall clock). Both are measured, not assumed.
- Retention: the in-memory ring only (<= 300 closed bins = 5 min). `/history` beyond that is an extension, not in fv.1 yet.
"""
from __future__ import annotations

from typing import Optional

from pulse.ingest.rings import Bin

SCHEMA_VERSION = "fv.1"

# (field, allowed types). Order here = JSON key order. Frozen by tests/test_features.py.
FIELDS: tuple[tuple[str, tuple], ...] = (
    ("schema_version", (str,)), ("venue", (str,)), ("symbol", (str,)), ("ts_ms", (int,)), ("bin", (str,)),
    ("mid", (float, type(None))), ("microprice", (float, type(None))), ("spread_abs", (float, type(None))),
    ("obi_top1", (float, type(None))), ("obi_top5", (float, type(None))), ("effective_spread", (float, type(None))),
    ("realized_vol_60s", (float, type(None))), ("quote_rate_1s", (int,)), ("trade_count_1s", (int,)),
    ("avg_trade_size_60s", (float, type(None))), ("empty", (bool,)), ("induced", (bool,)), ("provenance", (dict,)),
)
PROVENANCE_FIELDS = ("derived_lag_ms_p50", "pulse_age_ms")


def vector(b: Bin, now_wall_ms: int) -> dict:
    tot = (b.stages or {}).get("tot") or {}
    return {
        "schema_version": SCHEMA_VERSION, "venue": b.venue, "symbol": b.symbol, "ts_ms": b.close_ts_ms, "bin": "1s",
        "mid": b.mid, "microprice": b.microprice, "spread_abs": b.spread_abs,
        "obi_top1": b.obi_top1, "obi_top5": b.obi_top5, "effective_spread": b.effective_spread,
        "realized_vol_60s": b.realized_vol_60s, "quote_rate_1s": b.n_derived, "trade_count_1s": b.trade_count_1s,
        "avg_trade_size_60s": b.avg_trade_size_60s, "empty": b.empty, "induced": b.induced,
        "provenance": {"derived_lag_ms_p50": tot.get("p50"), "pulse_age_ms": max(0, now_wall_ms - b.close_ts_ms)},
    }


def latest(bins: list[Bin], now_wall_ms: int) -> Optional[dict]:
    return vector(bins[-1], now_wall_ms) if bins else None


def window(bins: list[Bin], now_wall_ms: int, from_ms: Optional[int] = None, to_ms: Optional[int] = None) -> list[dict]:
    """Vectors whose bin close is in (from_ms, to_ms], oldest first."""
    return [vector(b, now_wall_ms) for b in bins
            if (from_ms is None or b.close_ts_ms > from_ms) and (to_ms is None or b.close_ts_ms <= to_ms)]
