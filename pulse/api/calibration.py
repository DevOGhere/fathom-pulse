"""Calibration self-test: does Pulse find a delay we hide on purpose?  (the X factor)

Scope, stated plainly: this validates PULSE'S OWN measurement chain at the stamp sites Pulse owns (stage 2 consume stamp,
stage 3 frame-build stamp) plus the stage maths, binning and attribution. It cannot validate Fathom's upstream stamps
(stage 1 belongs to Fathom and is never injected).

Each run: a SHAM window (nothing injected; must report "nothing found") then a REAL window in which a known delay is
injected at a real stamp site of a randomly chosen stage (2 or 3). The REAL window is labelled `induced`: frames and bins
carry the flag and it is excluded from SLO/histograms. Stage 2 holds each derived message in the poll thread and takes its
consume stamp D later; stage 3 releases each record to the aggregator D after its consume stamp (absolute scheduling, so a
backlog cannot compound the delay).
"""
from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from typing import Optional

from pulse.api.calib_math import judge

D_MS = 80.0
WINDOW_S = 4.0
BASE_S = 30.0
TTL_S = 180          # retest happens BEFORE expiry, so "expired" means the tests stopped, not a healthy cycle
RETEST_S = 150


class Calibrator:
    def __init__(self, hub) -> None:
        self.hub = hub
        self.history: deque[dict] = deque(maxlen=30)
        self.running = False
        self.phase = ""
        self.streak = 0
        self._rng = random.Random()

    def snapshot(self) -> dict:
        last = self.history[-1] if self.history else None
        now_ms = int(time.time() * 1000)
        if self.running:
            status = "running"
        elif last is None:
            status = "none"
        elif not last["ok"]:
            status = "inconclusive" if last.get("inconclusive") else "failed"
        elif now_ms - last["at_ms"] > TTL_S * 1000:
            status = "expired"
        else:
            status = "valid"
        return {"status": status, "phase": self.phase if self.running else "", "ttl_s": TTL_S,
                "age_s": None if last is None else round((now_ms - last["at_ms"]) / 1000, 1),
                "streak": self.streak, "runs": len(self.history), "passed": sum(1 for h in self.history if h["ok"]),
                "last": last, "injecting": self.hub.injecting()}

    def _gather(self, t_from_ns: int, t_to_ns: int, induced: bool) -> dict[int, list[float]]:
        out = {1: [], 2: [], 3: []}
        for v, s in self.hub.agg.keys():
            for e in self.hub.agg.stage_samples(v, s, t_from_ns, t_to_ns):
                if bool(e[4]) == induced:
                    out[1].append(e[1]); out[2].append(e[2]); out[3].append(e[3])
        return out

    async def run_once(self, stage: Optional[int] = None, d_ms: float = D_MS) -> dict:
        if self.running:
            return {"error": "already running"}
        self.running = True
        stage = stage or self._rng.choice([2, 3])
        try:
            ns = 1_000_000_000
            self.phase = "sham"
            t0 = time.time_ns()
            await asyncio.sleep(WINDOW_S + 0.5)
            base = self._gather(t0 - int(BASE_S * ns), t0, induced=False)
            sham = judge(base, self._gather(t0 + int(0.5 * ns), t0 + int((WINDOW_S + 0.5) * ns), induced=False), expected=None)
            self.phase = f"injecting {d_ms:.0f} ms into stage {stage}"
            self.hub.consumer_inject_reset()
            t1 = time.time_ns()
            self.hub.inject(stage, d_ms)
            await asyncio.sleep(WINDOW_S + 0.8)
            self.hub.inject(None, 0)
            truth = self.hub.consumer_inject_mean(stage)       # ACTUAL injected delay (measured), not the nominal D
            await asyncio.sleep(0.6)                       # let held messages and pending stage-3 stamps flush
            test = self._gather(t1 + int(0.8 * ns), t1 + int((WINDOW_S + 0.8) * ns), induced=True)
            real = judge(base, test, expected=stage, truth_ms=truth or d_ms)
            # A sham that finds movement means the system itself was not steady (e.g. an upstream latency excursion) while we
            # watched nothing being injected. The self-test then cannot be trusted either way: inconclusive + retry, NOT "failed".
            unsteady = sham.get("status") == "judged" and not sham["ok"]
            ok = bool(real["ok"] and sham["ok"])
            if unsteady:
                real["reason"] = f"conditions were not steady: {sham.get('reason')}. Nothing was injected, so this self-test cannot be trusted; retrying"
            rec = {"at_ms": int(time.time() * 1000), "ok": ok, "stage": stage, "d_ms": d_ms, "truth_ms": round(truth or d_ms, 2),
                   "found": real.get("found"), "deltas_ms": real.get("deltas_ms"), "se_ms": real.get("se_ms"), "z_sep": real.get("z_sep"),
                   "confidence": real.get("confidence"), "size_err_ms": real.get("size_err_ms"), "tol_ms": real.get("tol_ms"),
                   "reason": real.get("reason"), "sham_ok": sham["ok"], "sham_reason": sham.get("reason"),
                   "inconclusive": unsteady or real.get("status") == "inconclusive" or sham.get("status") == "inconclusive",
                   "n_base": real.get("n_base"), "n_test": real.get("n_test")}
            self.history.append(rec)
            if rec["inconclusive"]:
                self.hub.log_event("warning", f"Self-test inconclusive: {real.get('reason')}")
            else:
                self.streak = self.streak + 1 if ok else 0
                self.hub.log_event("good" if ok else "critical", f"Self-test {'passed' if ok else 'FAILED'}: {real.get('reason')}")
            return rec
        finally:
            self.hub.inject(None, 0)
            self.running = False
            self.phase = ""

    async def loop(self) -> None:
        await asyncio.sleep(12)
        while True:
            try:
                r = await self.run_once()
                if r.get("inconclusive"):
                    await asyncio.sleep(20)                  # retry soon: an unsteady moment should not leave the badge amber for minutes
                    continue
            except Exception as e:                           # a failed run must not kill the schedule
                self.hub.log_event("critical", f"Self-test crashed: {type(e).__name__}: {e}"[:160])
            await asyncio.sleep(RETEST_S)
