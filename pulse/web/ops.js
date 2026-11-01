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
  // ---- copy: what each figure means, in plain words, with the live number in it. Rules (enforced in tests/ops_logic_check.js):
  // one or two short sentences, the real figure included, no filler or hype words, no exclamation marks. ----
  const fmtMs = ms => (ms < 1 ? 'under 1 ms' : ms < 1000 ? `${Math.round(ms)} ms` : ms < 60000 ? `${(ms / 1000).toFixed(1)} s` : `${(ms / 60000).toFixed(1)} min`);
  const VN = k => ({ binance: 'Binance', coinbase: 'Coinbase' }[String(k).split('/')[0]] || k);
  const EXPLAIN = {
    fresh: (k, b, ms) => ({ h: `${VN(k)} data: ${b.word}`, t: {
      Fresh: `The newest ${VN(k)} update arrived ${fmtMs(ms)} ago. Under 200 ms counts as fresh.`,
      Aging: `The newest ${VN(k)} update is ${fmtMs(ms)} old. Updates normally land within 200 ms.`,
      Stale: `Nothing new from ${VN(k)} for ${fmtMs(ms)}. The price on screen may be out of date.`,
      'Not to be trusted': `${VN(k)} has been silent for ${fmtMs(ms)}. Treat its price as unknown until updates resume.` }[b.word] }),
    rate: (k, b, ratio) => ({ h: `${VN(k)} updates: ${b.word}`, t: `Updates per second against ${VN(k)}'s own normal over the last minute: now ${Math.round((ratio || 0) * 100)}% of normal. ` + {
      Normal: 'That is within its usual range.', Quiet: 'Fewer than usual. Often a calm market, sometimes a feed problem.', Collapsed: 'Almost nothing is arriving.' }[b.word] }),
    speed: (b, ms) => ({ h: `Speed: ${b.word}`, t: `How long an update takes from reaching Fathom to being ready in Pulse. The slowest 1 in 100 over the last minute took about ${fmtMs(OPS.roundTo(ms, 10))}. Smooth is under 50 ms, Normal under 100, Slow under 200.` }),
    backlog: (b, n) => ({ h: `Backlog: ${b.word}`, t: `Fathom's processor reads updates from a queue, and ${Math.round(n)} are waiting now. Under 50 means it is keeping up; over 200 means everything you see is getting older.` }),
    trust: (st, ageS, ttlS, tol) => ({ h: `Trust: ${st === 'valid' ? 'Verified' : st === 'running' ? 'Checking' : 'Unverified'}`, t: {
      valid: `Every few minutes Pulse hides a known 80 ms delay inside itself and checks that it can find it. The last check passed ${fmtMs(ageS * 1000)} ago, within ±${tol} ms, and holds for about ${fmtMs(Math.max(0, ttlS - ageS) * 1000)} more.`,
      running: 'A self-test is running right now. Latency figures are delayed on purpose until it ends.',
      expired: 'The last passing check is too old to rely on. Latency figures are unverified until the next one.',
      failed: 'The last self-test did not find the delay it hid. Treat latency figures as unverified.',
      inconclusive: 'The last self-test could not tell, usually because the feed was not steady. It retries on its own.',
      none: 'No self-test has finished yet.' }[st] || 'No self-test has finished yet.' }),
    price: k => ({ h: `${VN(k)} price`, t: 'Price at the close of the last full second. The Detail view shows every live tick.' }),
    chart: {
      latency: { h: 'Pipeline speed', t: 'Second by second: the typical time (line) and the slowest 1 in 100 (top of the shaded band) for an update to get from Fathom into Pulse. The coloured zones behind are the Smooth, Normal, Slow and Late ranges.' },
      price: { h: 'Price', t: 'Mid price: halfway between the best buy and best sell, smoothed over 3 seconds. Hover for the exact second.' },
      pressure: { h: 'Buy and sell pressure', t: 'Who is leaning on the order book each second. Blue means more resting buy size, red more resting sell size, grey is balanced.' },
      avail: { h: 'Feed activity', t: 'Updates received each second against the feed\'s own normal. Green is normal, amber quiet, orange sparse, dark red nothing at all. A red tick above marks an incident.' },
      trades: { h: 'Trades', t: 'Trades per 5 seconds, both venues together.' },
      share: { h: 'Where the time goes', t: 'Average time per stage over the last minute: Fathom (receive and derive), the hand-off to Pulse, and Pulse itself. The grey part is the wait for the next screen refresh, shown apart because it is a display choice.' },
    },
    stage: {
      s1: { h: 'Fathom', t: 'From Fathom receiving the exchange message to it having computed the derived numbers.' },
      s2: { h: 'Hand-off to Pulse', t: 'From Fathom publishing those numbers to Pulse reading them off the queue.' },
      s3: { h: 'Pulse', t: 'How long Pulse then takes to have the update ready for a screen refresh. Usually far under a millisecond.' },
      tw: { h: 'Wait for the next refresh', t: 'The screen refreshes ten times a second, so an update waits up to 100 ms for its turn. It is a display choice, not processing, so it is kept out of the speed rating.' },
    },
    head: parts => ({ h: 'What this is based on', t: parts.join('\n') }),
    ranges: name => BANDS[name].map((b, i, a) => `${b[2]} ${i === a.length - 1 ? 'beyond ' + a[i - 1][0] : 'under ' + b[0]}`).join(' · '),
  };
  const BANNED = /\b(seamless|leverage|robust|cutting-edge|delve|unlock|empower|journey|simply|just|ensure|effortless|powerful|magic)\b|!/i;
  return { EXPLAIN, BANNED, fmtMs, VN, BANDS, SEV, bandIndex, band, median, Hold, Ema, roundTo, worst, headline, rateRatio, WORSE_S, BETTER_S };
})();
if (typeof module !== 'undefined') module.exports = OPS;
