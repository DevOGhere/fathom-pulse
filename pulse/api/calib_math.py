"""Calibration verdict maths (pure, no I/O) — so the test of the test can itself be tested.

A self-test compares per-record stage durations (ms) in a BASELINE window with those in a TEST window in which a delay
was injected at a real Pulse stamp site. Per stage: delta = mean(test) - mean(base), se = Welch standard error. The
tolerance comes from the measured noise (se), never from a fixed number: stage 3 is quantised by the 100 ms frame
tick (roughly uniform over 0-100 ms) so a fixed +-3 ms band would fail a healthy system.

Verdicts
- real injection at stage E (expected): pass only if the found stage == E, the top stage is separated from the runner-up
  by >= Z_SEP combined standard errors (else "not sure"), the top delta is > Z_DETECT se, and the size error
  |delta_E - truth| <= max(TOL_FLOOR_MS, TOL_K * se_E).
- sham (expected None, nothing injected): pass only if NO stage shows delta > Z_DETECT se ("nothing found").
- negative control: judging the same data against the wrong expected stage must FAIL (see tests).
"""
from __future__ import annotations

import math
from typing import Optional

STAGES = (1, 2, 3)
Z_DETECT = 3.0
Z_SEP = 3.0
TOL_K = 3.0
TOL_FLOOR_MS = 2.0
MIN_N = 20
SE_FLOOR = 0.05


def _mean(v):
    return sum(v) / len(v)


def _var(v):
    m = _mean(v)
    return sum((x - m) ** 2 for x in v) / (len(v) - 1) if len(v) > 1 else 0.0


def welch(base: list[float], test: list[float]) -> tuple[float, float]:
    d = _mean(test) - _mean(base)
    se = math.sqrt(_var(base) / len(base) + _var(test) / len(test))
    return d, max(se, SE_FLOOR)


def judge(base: dict[int, list[float]], test: dict[int, list[float]], expected: Optional[int], truth_ms: float = 0.0) -> dict:
    if any(len(base.get(s, [])) < MIN_N or len(test.get(s, [])) < MIN_N for s in STAGES):
        return {"status": "inconclusive", "ok": False, "reason": f"need >= {MIN_N} samples per stage per window",
                "n_base": min(len(base.get(s, [])) for s in STAGES), "n_test": min(len(test.get(s, [])) for s in STAGES)}
    d, se = {}, {}
    for s in STAGES:
        d[s], se[s] = welch(base[s], test[s])
    order = sorted(STAGES, key=lambda s: d[s], reverse=True)
    top, second = order[0], order[1]
    sep = (d[top] - d[second]) / math.sqrt(se[top] ** 2 + se[second] ** 2)
    detected = d[top] > Z_DETECT * se[top]
    pos = {s: max(0.0, d[s]) for s in STAGES}
    conf = pos[top] / sum(pos.values()) if sum(pos.values()) > 0 else None
    out = {"status": "judged", "deltas_ms": {s: round(d[s], 3) for s in STAGES}, "se_ms": {s: round(se[s], 3) for s in STAGES},
           "z_sep": round(sep, 2), "n_base": min(len(base[s]) for s in STAGES), "n_test": min(len(test[s]) for s in STAGES)}
    if expected is None:                                   # sham: nothing was injected
        out.update(found=None, ok=not detected, confidence=None, size_err_ms=None, tol_ms=None,
                   reason="nothing found, as expected" if not detected else f"false alarm: stage {top} moved {d[top]:.1f} ms ({d[top] / se[top]:.1f} se)")
        return out
    not_sure = detected and sep < Z_SEP
    found = top if (detected and not not_sure) else None
    size_err = d[expected] - truth_ms
    tol = max(TOL_FLOOR_MS, TOL_K * se[expected])
    ok = found == expected and abs(size_err) <= tol
    if not detected:
        reason = "injected delay not detected"
    elif not_sure:
        reason = f"not sure: stage {top} ({conf and round(conf * 100)}%) vs stage {second}; z_sep {sep:.1f} < {Z_SEP}"
    elif found != expected:
        reason = f"blamed stage {found}, delay was in stage {expected}"
    elif abs(size_err) > tol:
        reason = f"right stage, wrong size: error {size_err:+.1f} ms exceeds +-{tol:.1f} ms"
    else:
        reason = f"found +{d[expected]:.1f} ms in stage {expected} (truth {truth_ms:.1f}, error {size_err:+.1f} +-{tol:.1f} ms)"
    out.update(found=found, ok=ok, confidence=None if conf is None else round(conf, 3), size_err_ms=round(size_err, 3), tol_ms=round(tol, 3), reason=reason)
    return out
