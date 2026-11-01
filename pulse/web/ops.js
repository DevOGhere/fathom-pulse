'use strict';
/* Ops mode logic (pure; no DOM). Turns fast, noisy measurements into calm categories.
   - bands are the SAME thresholds the detector and the data-age chips use (docs/METRICS.md), just named.
   - Hold adds hysteresis: a category gets worse only after WORSE_S consecutive seconds beyond it, better after BETTER_S.
   - everything is sampled at 1 Hz by the caller, from the frame's own clock, so a recording replays identically. */
const OPS = (() => {
  const BANDS = {                       // [upper bound (exclusive), severity, word]
    fresh:   [[200, 'good', 'Fresh'], [500, 'warning', 'Aging'], [2000, 'serious', 'Stale'], [Infinity, 'critical', 'Not to be trusted']],
    speed:   [[50, 'good', 'Smooth'], [100, 'good', 'Normal'], [200, 'warning', 'Slow'], [Infinity, 'serious', 'Late']],
    backlog: [[50, 'good', 'Keeping up'], [200, 'warning', 'Busy'], [Infinity, 'serious', 'Behind']],
    rate:    [[0.4, 'serious', 'Collapsed'], [0.7, 'warning', 'Quiet'], [Infinity, 'good', 'Normal']],   // ratio to the feed's own last-minute median
  };
  const SEV = { good: 0, warning: 1, serious: 2, critical: 3 };
  const WORSE_S = 3, BETTER_S = 5;

  const bandIndex = (name, v) => BANDS[name].findIndex(b => v < b[0]);
  const band = (name, i) => ({ sev: BANDS[name][i][1], word: BANDS[name][i][2], i });
  const median = a => { if (!a.length) return null; const s = [...a].sort((x, y) => x - y), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };

  class Hold {                          // feed it one band index per second; read the held one
    constructor(worseS = WORSE_S, betterS = BETTER_S) { this.w = worseS; this.b = betterS; this.cur = null; this.run = 0; this.cand = null; }
    push(i) {
      if (this.cur === null || i === this.cur) { this.cur = this.cur === null ? i : this.cur; this.run = 0; this.cand = null; return this.cur; }
      this.run = this.cand === i ? this.run + 1 : 1; this.cand = i;
      if (this.run >= (i > this.cur ? this.w : this.b)) { this.cur = i; this.run = 0; this.cand = null; }
      return this.cur;
    }
  }
  class Ema { constructor(a = 0.2) { this.a = a; this.v = null; } push(x) { if (x == null) return this.v; this.v = this.v == null ? x : this.v + this.a * (x - this.v); return this.v; } }

  const roundTo = (v, step) => Math.round(v / step) * step;
  const worst = list => list.reduce((w, s) => (SEV[s] > SEV[w] ? s : w), 'good');
  function headline(sevs, openReal, induced) {
    if (induced) return { sev: 'warning', word: 'Test in progress', sub: 'A deliberate test is running; numbers are delayed on purpose.' };
    const w = worst(sevs.concat(openReal ? ['serious'] : []));
    return { good: { sev: w, word: 'All good', sub: 'Data is fresh and the pipeline is keeping up.' },
             warning: { sev: w, word: 'Slower than usual', sub: 'Still flowing; something is running behind its normal pace.' },
             serious: { sev: w, word: 'Degraded', sub: 'A feed or the pipeline is not healthy. Check the incidents below.' },
             critical: { sev: w, word: 'Feed problem', sub: 'Data is too old to trust right now.' } }[w];
  }
  const rateRatio = bins => { const n = (bins || []).filter(x => x != null); if (n.length < 20) return null; const base = median(n.slice(0, -5)), fast = n.slice(-5).reduce((a, c) => a + c, 0) / 5; return base >= 3 ? fast / base : null; };
  return { BANDS, SEV, bandIndex, band, median, Hold, Ema, roundTo, worst, headline, rateRatio, WORSE_S, BETTER_S };
})();
if (typeof module !== 'undefined') module.exports = OPS;
