'use strict';
/* FathomPulse live page. Rules: (1) NO synthetic data: a panel whose data is not in the frame says so.
   (2) receiving != drawing: onmessage only stores the latest frame; one rAF loop draws when something changed.
   (3) canvases are sized on resize only. (4) when the socket drops, numbers freeze and say "disconnected". */

const $ = id => document.getElementById(id);
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const C = { grid: 'rgba(255,255,255,0.10)', base: 'rgba(255,255,255,0.24)', muted: '#a7a69e', sec: css('--ink-secondary'),
  bid: css('--bid'), ask: css('--ask'), seq: css('--seq-350'), ring: 'rgba(10,14,18,0.88)' };

// data-age thresholds on derived_age_ms (now - processor derived_at, bridged). Calibrated 2026-10-29 from a 90 s capture:
// binance p50/p90/p99 = 53/154/354 ms, coinbase 68/114/244 ms. The number includes the wait between updates
// (binance publishes every 100 ms, so ~50 ms of average age is structural, not lag).
const AGE = [[200, 'good', 'fresh', '●'], [500, 'warning', 'aging', '▲'], [2000, 'serious', 'stale', '■'], [Infinity, 'critical', 'not to be trusted', '✕']];
const ageClass = ms => AGE.find(a => ms < a[0]);
const QUOTE = { binance: 'USDT', coinbase: 'USD' };      // Binance quotes in USDT, not USD

const S = { frame: null, sel: null, tail: {}, lagHist: [], connected: false, everConnected: false, dirty: true,
  lastSeq: 0, seqGaps: 0, pct: 99, loc: {}, hits: [], pinned: null, srvEv: [], lastDrops: 0, lagHigh: false, log: [], draws: 0, msgs: 0, costs: [], rafTicks: 0, lastStale: false };
const TAIL_MS = 60000, TAIL_MAX = 700;

function logEv(level, text) { S.log.unshift({ t_ms: Date.now(), level, text }); if (S.log.length > 40) S.log.pop(); S.logDirty = true; }
const LOCAL = ['localhost', '127.0.0.1', '[::1]'].includes(location.hostname);   // stage 4 is only meaningful on the same host clock

// ---------- socket ----------
let ws, backoff = 500;
function connect() {
  ws = new WebSocket(`ws://${location.host}/stream`);
  ws.onopen = () => { S.connected = true; backoff = 500; S.tail = {}; S.lagHist = []; S.lastSeq = 0;
    logEv('good', S.everConnected ? 'Reconnected; history rebuilt from the frame' : 'Connected to Pulse'); S.everConnected = true; S.dirty = true; };
  ws.onmessage = e => { const f = JSON.parse(e.data); S.msgs++; ingest(f); };
  ws.onclose = () => { if (S.connected) logEv('critical', 'Lost the Pulse stream. Numbers are frozen, not current.'); S.connected = false; S.dirty = true;
    setTimeout(connect, backoff); backoff = Math.min(backoff * 2, 5000); };
  ws.onerror = () => ws.close();
}
function ingest(f) {
  if (S.lastSeq && f.seq !== S.lastSeq + 1) S.seqGaps += f.seq - S.lastSeq - 1;
  S.lastSeq = f.seq; S.frame = f;
  if (LOCAL && f.client.sent_ms) { const sec = Math.floor(Date.now() / 1000); (S.loc[sec] || (S.loc[sec] = [])).push(Date.now() - f.client.sent_ms);
    for (const k of Object.keys(S.loc)) if (+k < sec - 70) delete S.loc[k]; }
  S.srvEv = f.events || [];
  if (!S.sel) S.sel = Object.keys(f.series)[0] || null;
  for (const [k, s] of Object.entries(f.series)) {
    const t = S.tail[k] || (S.tail[k] = []);
    if (!t.length && s.bins) s.bins.start_s.forEach((st, i) => { if (s.bins.mid[i] != null) t.push({ t: (st + 1) * 1000, mid: s.bins.mid[i], micro: s.bins.microprice[i], obi: s.bins.obi_top1[i] }); });  // rebuild from the frame's own bins
    const w = s.window_1s;
    if (w && (!t.length || t[t.length - 1].t < f.ts_ms)) t.push({ t: f.ts_ms, mid: w.mid, micro: w.microprice, obi: w.obi_top1 });
    while (t.length && (t[0].t < f.ts_ms - TAIL_MS || t.length > TAIL_MAX)) t.shift();
  }
  S.lagHist.push({ t: f.ts_ms, lag: f.lag.total }); while (S.lagHist.length && S.lagHist[0].t < f.ts_ms - TAIL_MS) S.lagHist.shift();
  if (f.client.drops > S.lastDrops) { logEv('warning', `Dropped ${f.client.drops - S.lastDrops} frame(s) for this client; state kept, smoothness lost`); S.lastDrops = f.client.drops; }
  const lag = f.lag.total;
  if (lag != null && lag > 400 && !S.lagHigh) { S.lagHigh = true; logEv('serious', `Processor lag crossed 400 msgs (${lag})`); }
  if (lag != null && lag < 150 && S.lagHigh) { S.lagHigh = false; logEv('good', `Processor lag recovered (${lag})`); }
  const a = f.pulse_age_ms; const stale = a != null && a > 2000;
  if (stale !== S.lastStale) { S.lastStale = stale; logEv(stale ? 'critical' : 'good', stale ? `No new data for ${(a / 1000).toFixed(1)} s` : 'Data flowing again'); }
  S.dirty = true;
}

// ---------- canvas ----------
const cvs = {};
function fit(id) {
  const c = $(id), dpr = window.devicePixelRatio || 1, h = +c.dataset.h, w = c.clientWidth;
  c.width = Math.round(w * dpr); c.height = Math.round(h * dpr); c.style.height = h + 'px';
  const x = c.getContext('2d'); x.setTransform(dpr, 0, 0, dpr, 0, 0); cvs[id] = { c, x, w, h };
}
const ids = ['cMid', 'cObi', 'cQuote', 'cLag', 'cAnat', 'cHist'];
function fitAll() { ids.forEach(fit); S.dirty = true; }
new ResizeObserver(fitAll).observe(document.body);

const PAD = { l: 48, r: 10, t: 6, b: 18 };
const px = (g, t, now) => PAD.l + (g.w - PAD.l - PAD.r) * (1 - (now - t) / TAIL_MS);
const py = (g, v, a, b) => PAD.t + (g.h - PAD.t - PAD.b) * (1 - (v - a) / ((b - a) || 1));
function axes(g, a, b, ticks, fmt, xl = true) {
  const x = g.x; x.clearRect(0, 0, g.w, g.h); x.strokeStyle = C.grid; x.lineWidth = 1; x.fillStyle = C.muted; x.font = '10px ' + css('--font-ui'); x.textAlign = 'right'; x.textBaseline = 'middle';
  for (let i = 0; i <= ticks; i++) { const v = a + (b - a) * i / ticks, y = PAD.t + (g.h - PAD.t - PAD.b) * (1 - i / ticks);
    x.beginPath(); x.moveTo(PAD.l, Math.round(y) + .5); x.lineTo(g.w - PAD.r, Math.round(y) + .5); x.stroke(); x.fillText(fmt(v), PAD.l - 6, y); }
  if (xl) { x.textAlign = 'center'; x.textBaseline = 'top'; [0, 15, 30, 45, 60].forEach(s => x.fillText(s === 60 ? 'now' : `−${60 - s}s`, PAD.l + (g.w - PAD.l - PAD.r) * s / 60, g.h - PAD.b + 4)); }
}
function emptyChart(g, msg) { const x = g.x; x.clearRect(0, 0, g.w, g.h); x.fillStyle = C.muted; x.font = '12px ' + css('--font-ui'); x.textAlign = 'center'; x.textBaseline = 'middle'; x.fillText(msg, g.w / 2, g.h / 2); }
const sel = () => S.frame && S.sel ? S.frame.series[S.sel] : null;
const dp = k => (k && k.startsWith('binance') ? 2 : 2);

function drawMid(now) {
  const g = cvs.cMid, t = S.tail[S.sel] || []; if (t.length < 2) return emptyChart(g, 'Waiting for data…');
  const all = t.flatMap(p => [p.mid, p.micro]), lo = Math.min(...all), hi = Math.max(...all), pad = (hi - lo) * .12 || 1, a = lo - pad, b = hi + pad;
  const dec = (b - a) < 0.5 ? 3 : (b - a) < 5 ? 2 : (b - a) < 50 ? 1 : 0;   // enough decimals that adjacent ticks differ
  axes(g, a, b, 4, v => v.toFixed(dec));
  const x = g.x; x.lineWidth = 2; x.lineJoin = x.lineCap = 'round';
  x.strokeStyle = C.sec; x.beginPath(); t.forEach((p, i) => { const X = px(g, p.t, now), Y = py(g, p.mid, a, b); i ? x.lineTo(X, Y) : x.moveTo(X, Y); }); x.stroke();
  for (let i = 1; i < t.length; i++) { x.strokeStyle = t[i].micro >= t[i].mid ? C.bid : C.ask; x.beginPath(); x.moveTo(px(g, t[i - 1].t, now), py(g, t[i - 1].micro, a, b)); x.lineTo(px(g, t[i].t, now), py(g, t[i].micro, a, b)); x.stroke(); }
  const l = t[t.length - 1], X = px(g, l.t, now), Y = py(g, l.mid, a, b); x.fillStyle = C.ring; x.beginPath(); x.arc(X, Y, 6, 0, 7); x.fill(); x.fillStyle = C.sec; x.beginPath(); x.arc(X, Y, 4, 0, 7); x.fill();
}
function drawObi(now) {
  const g = cvs.cObi, t = S.tail[S.sel] || []; if (!t.length) return emptyChart(g, 'Waiting for data…');
  axes(g, -1, 1, 2, v => v.toFixed(1), false); const x = g.x, z = py(g, 0, -1, 1), bw = Math.max(1, (g.w - PAD.l - PAD.r) / TAIL_MAX * 0.9);
  t.forEach(p => { if (p.obi == null) return; const X = px(g, p.t, now), Y = py(g, p.obi, -1, 1); x.fillStyle = p.obi >= 0 ? C.bid : C.ask; x.fillRect(X - bw, Math.min(z, Y), bw, Math.abs(Y - z)); });
  x.strokeStyle = C.base; x.beginPath(); x.moveTo(PAD.l, Math.round(z) + .5); x.lineTo(g.w - PAD.r, Math.round(z) + .5); x.stroke();
}
function drawTimeline(now) {
  const s = sel(), g1 = cvs.cQuote, g2 = cvs.cLag;
  if (!s || !s.bins) { emptyChart(g1, 'Waiting for data…'); emptyChart(g2, 'Waiting for data…'); return; }
  const b = s.bins, rmax = Math.max(20, ...b.n_derived); axes(g1, 0, rmax, 2, v => v.toFixed(0) + '/s', false);
  const x = g1.x; x.strokeStyle = C.seq; x.lineWidth = 2; x.lineJoin = 'round'; x.beginPath();
  b.start_s.forEach((st, i) => { const X = px(g1, (st + 1) * 1000, now), Y = py(g1, b.n_derived[i], 0, rmax); i ? x.lineTo(X, Y) : x.moveTo(X, Y); }); x.stroke();
  const lh = S.lagHist.filter(p => p.lag != null); if (lh.length < 2) return emptyChart(g2, S.frame.lag.ok ? 'Waiting for lag samples…' : 'Lag unavailable: ' + (S.frame.lag.error || 'unknown'));
  const lmax = Math.max(50, ...lh.map(p => p.lag)); axes(g2, 0, lmax, 3, v => v.toFixed(0));
  const y = g2.x; y.strokeStyle = C.sec; y.lineWidth = 2; y.lineJoin = 'round'; y.beginPath(); lh.forEach((p, i) => { const X = px(g2, p.t, now), Y = py(g2, p.lag, 0, lmax); i ? y.lineTo(X, Y) : y.moveTo(X, Y); }); y.stroke();
}

function drawHist() {
  const g = cvs.cHist, sl = S.frame && S.frame.slo, h = sl && sl.hist;
  if (!h || !sl.n) return emptyChart(g, 'Waiting for records…');
  const x = g.x, w = g.w, ht = g.h, L = 34, R = 8, T = 8, B = 20, n = h.counts.length, cmax = Math.max(1, ...h.counts), bw = (w - L - R) / n;
  x.clearRect(0, 0, w, ht); x.font = '11px ui-monospace, monospace'; x.textBaseline = 'middle';
  x.strokeStyle = C.grid; x.fillStyle = C.muted; x.textAlign = 'right';
  for (const f of [0, 0.5, 1]) { const y = Math.round(T + (ht - T - B) * (1 - f)) + .5; x.beginPath(); x.moveTo(L, y); x.lineTo(w - R, y); x.stroke(); x.fillText(Math.round(cmax * f), L - 4, y); }
  const tgt = h.edges_ms.indexOf(sl.target_ms);
  h.counts.forEach((c, i) => { const bh = (ht - T - B) * c / cmax; x.fillStyle = i >= tgt && tgt >= 0 ? css('--status-serious') : C.seq; x.fillRect(L + i * bw + 1, ht - B - bh, Math.max(1, bw - 2), bh); });
  x.fillStyle = C.muted; x.textAlign = 'center';
  h.edges_ms.forEach((e, i) => { if (i % 2 === 0 || i === n - 1) x.fillText(i === n - 1 ? e + '+' : e, L + i * bw + bw / 2, ht - 8); });
  if (tgt >= 0) { const X = Math.round(L + tgt * bw) + .5; x.strokeStyle = css('--status-serious'); x.setLineDash([4, 3]); x.beginPath(); x.moveTo(X, T); x.lineTo(X, ht - B); x.stroke(); x.setLineDash([]); }
  $('histNote').textContent = `ms per record, ${sl.n} records · p99 ${sl.p99_ms} ms · target ${sl.target_ms} ms (dashed). Bars right of the line break the target. Tick wait (≈${sl.tick_wait_p50_ms} ms median) is not included.`;
}

// ---------- DOM panels ----------
const el = (tag, cls, text) => { const e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };
function spark(vals) { const v = vals.filter(x => x != null); if (v.length < 2) return ''; const lo = Math.min(...v), hi = Math.max(...v), r = hi - lo || 1;
  return `<svg width="70" height="18" aria-hidden="true"><polyline fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" points="${v.map((q, i) => `${(i / (v.length - 1) * 70).toFixed(1)},${(17 - (q - lo) / r * 16).toFixed(1)}`).join(' ')}"/></svg>`; }
function renderStrip(f) {
  const strip = $('strip'); strip.textContent = '';
  for (const [k, s] of Object.entries(f.series)) {
    const mids = s.bins ? s.bins.mid : [], chg = mids.length > 1 && mids[0] ? (mids[mids.length - 1] / mids.find(v => v != null) - 1) * 100 : null;
    const b = el('button', 'pill'); b.setAttribute('aria-pressed', k === S.sel); b.dataset.k = k;
    const last = s.window_1s ? s.window_1s.mid : null;
    b.append(el('b', '', k), el('span', 'num', last != null ? last.toFixed(2) : '—'));
    const sp = document.createElement('span'); sp.innerHTML = spark(mids); b.append(sp);
    b.append(el('span', 'chg num', chg == null ? 'no history yet' : `${chg >= 0 ? '▲' : '▼'} ${Math.abs(chg).toFixed(2)}% · ${mids.length} s`));
    strip.append(b);
  }
}
$('strip').addEventListener('click', e => { const b = e.target.closest('.pill'); if (b) { S.sel = b.dataset.k; S.dirty = true; } });

function setChips(cls, glyph, text) { document.querySelectorAll('[data-chip]').forEach(ch => { ch.className = 'chip ' + cls; ch.textContent = `${glyph} ${text}`; }); }
function renderChrome(f, s, w) {
  const cb = $('connBadge'); cb.className = 'badge ' + (S.connected ? 'good' : 'critical'); $('connText').textContent = S.connected ? 'Live' : (S.everConnected ? 'Disconnected · numbers frozen' : 'Connecting…');
  if (w) { const [, cls, word, glyph] = ageClass(w.derived_age_ms); $('ageBadge').className = 'badge ' + cls; $('ageText').textContent = `${S.sel} · ${word} · ${w.derived_age_ms.toFixed(0)} ms old`; setChips(cls, glyph, `${w.derived_age_ms.toFixed(0)} ms · ${word}`); }
  else { $('ageBadge').className = 'badge'; $('ageText').textContent = 'no data yet'; setChips('', '○', 'no data'); }
  $('tLag').textContent = f.lag.total == null ? 'n/a' : f.lag.total; $('tLag').title = f.lag.ok ? '' : (f.lag.error || '');
  $('tRate').textContent = w ? w.n : '—'; $('tDrop').textContent = f.client.drops;
  if (w) { $('heroMid').textContent = w.mid.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    $('heroSub').textContent = `mid · ${QUOTE[S.sel.split('/')[0]] || '?'} · ${S.sel}`;
    $('spreadNote').textContent = `spread ${w.spread_abs.toFixed(2)} · ` + (w.effective_spread == null ? 'effective spread blank: no recent trade' : `effective spread ${w.effective_spread.toFixed(2)}`); }
}
const STAGE_COL = () => [css('--stage-recv'), css('--stage-derive'), css('--stage-pulse'), css('--stage-ws')];
const STAGE_NAME = ['1 · producer → derived (Fathom)', '2 · derived → Pulse consumed', '3 · consumed → frame built', '4 · sent → your screen (local estimate)'];
const q = (v, p) => { if (!v || !v.length) return null; const t = [...v].sort((x, y) => x - y); return t[Math.min(t.length - 1, Math.floor(p / 100 * t.length))]; };
function binStages(s, i) {   // [s1,s2,s3,s4|null] for bin i at the selected percentile
  const st = s.bins.stages, p = 'p' + S.pct, v = [st['s1_' + p][i], st['s2_' + p][i], st['s3_' + p][i]];
  if (v.some(x => x == null)) return null;
  const loc = LOCAL ? S.loc[s.bins.start_s[i]] : null; v.push(loc && loc.length >= 3 ? q(loc, S.pct) : null); return v;
}
function drawAnat(now) {
  const g = cvs.cAnat, s = sel(); if (!s || !s.bins || !s.bins.stages) return emptyChart(g, 'Waiting for data…');
  const n = s.bins.start_s.length, rows = []; let mx = 20;
  for (let i = 0; i < n; i++) { const v = binStages(s, i); rows.push(v); if (v) mx = Math.max(mx, v.reduce((a, c) => a + (c || 0), 0)); }
  const ymax = Math.ceil(mx / 20) * 20; axes(g, 0, ymax, 4, v => v.toFixed(0) + ' ms');
  const x = g.x, cols = STAGE_COL(), bw = Math.min(12, (g.w - PAD.l - PAD.r) / 60 - 2); S.hits = [];
  rows.forEach((v, i) => { const X = px(g, (s.bins.start_s[i] + 1) * 1000, now) - bw - 1; S.hits.push({ x0: X - 1, x1: X + bw + 1, i });
    if (!v) return; let acc = 0;
    v.forEach((val, k) => { if (val == null || val <= 0) return; const y1 = py(g, acc, 0, ymax), y0 = py(g, acc + val, 0, ymax), h = Math.max(1, y1 - y0 - 2); x.fillStyle = cols[k];
      if (k === 3 || (k === 2 && v[3] == null)) { const r = Math.min(4, h); x.beginPath(); x.moveTo(X, y1 - 2); x.lineTo(X, y0 + r); x.arcTo(X, y0, X + r, y0, r); x.lineTo(X + bw - r, y0); x.arcTo(X + bw, y0, X + bw, y0 + r, r); x.lineTo(X + bw, y1 - 2); x.fill(); }
      else x.fillRect(X, y0, bw, h); acc += val; });
    if (s.bins.induced[i]) { x.fillStyle = '#fff'; x.fillRect(X, PAD.t, bw, 2); }
    if (S.pinned === s.bins.start_s[i]) { x.strokeStyle = C.sec; x.lineWidth = 1; x.strokeRect(X - 2, PAD.t, bw + 4, g.h - PAD.t - PAD.b); } });
}
function attribution(s, i) {   // where did the EXTRA time go, relative to what is typical for this venue? (stage 4 excluded: local only)
  const st = s.bins.stages, p = 'p' + S.pct, quiet = [];
  for (let j = 0; j < s.bins.start_s.length; j++) if (!s.bins.induced[j] && st['s1_' + p][j] != null) quiet.push(j);
  const typ = ['s1', 's2', 's3'].map(k => q(quiet.map(j => st[k + '_' + p][j]), 50) || 0);
  const ex = ['s1', 's2', 's3'].map((k, m) => Math.max(0, st[k + '_' + p][i] - typ[m])), T = ex.reduce((a, c) => a + c, 0), ord = [0, 1, 2].sort((a, b) => ex[b] - ex[a]);
  if (T < 2) return 'Nothing unusual: every stage is within its typical range.';
  if ((ex[ord[0]] - ex[ord[1]]) / T < 0.25) return `Not sure. Stage ${ord[0] + 1} (${Math.round(ex[ord[0]] / T * 100)}%) or stage ${ord[1] + 1} (${Math.round(ex[ord[1]] / T * 100)}%): this data cannot separate them.`;
  return `${Math.round(ex[ord[0]] / T * 100)}% of the extra time is in stage ${ord[0] + 1} (${STAGE_NAME[ord[0]]}).`;
}
const tipEl = $('tip'), hitAt = e => { const r = $('cAnat').getBoundingClientRect(), X = e.clientX - r.left; return S.hits.find(h => X >= h.x0 - 3 && X <= h.x1 + 3); };
$('cAnat').addEventListener('pointermove', e => { const h = hitAt(e), s = sel(); if (!h || !s) { tipEl.style.display = 'none'; return; } const v = binStages(s, h.i); if (!v) { tipEl.style.display = 'none'; return; }
  const cols = STAGE_COL(); tipEl.textContent = ''; const hd = document.createElement('div'); hd.style.cssText = 'color:var(--ink-muted);margin-bottom:4px'; hd.textContent = `${Math.round(Date.now() / 1000 - s.bins.start_s[h.i])} s ago · P${S.pct}${s.bins.induced[h.i] ? ' · INDUCED (self-test)' : ''}`; tipEl.append(hd);
  v.forEach((val, k) => { const r = document.createElement('div'); r.className = 'row'; const l = document.createElement('span'); const kk = document.createElement('i'); kk.className = 'k'; kk.style.background = cols[k]; l.append(kk, document.createTextNode(STAGE_NAME[k]));
    const b = document.createElement('b'); b.className = 'num'; b.textContent = val == null ? (k === 3 ? 'n/a' : '—') : val.toFixed(1) + ' ms'; r.append(l, b); tipEl.append(r); });
  tipEl.style.display = 'block'; tipEl.style.left = Math.min(e.clientX + 14, innerWidth - 300) + 'px'; tipEl.style.top = (e.clientY + 14) + 'px'; });
$('cAnat').addEventListener('pointerleave', () => { tipEl.style.display = 'none'; });
$('cAnat').addEventListener('click', e => { const h = hitAt(e), s = sel(), pin = $('pin'); if (!h || !s) { S.pinned = null; pin.textContent = 'Click a bar to pin one second and read it stage by stage.'; S.dirty = true; return; }
  S.pinned = s.bins.start_s[h.i]; const st = s.bins.stages, p = 'p' + S.pct; const tot = st['s1_' + p][h.i] + st['s2_' + p][h.i] + st['s3_' + p][h.i];
  pin.textContent = `Pinned. ${S.pct === 99 ? 'P99' : 'P50'} server total ${tot.toFixed(1)} ms (stages 1-3, before the wait for the next 10 Hz frame, median ${st['tw_p50'][h.i] == null ? '—' : st['tw_p50'][h.i].toFixed(0)} ms). ${attribution(s, h.i)}${s.bins.induced[h.i] ? ' This second was part of a self-test, so the extra time is deliberate.' : ''}`; S.dirty = true; });
$('pctSeg').addEventListener('click', e => { const b = e.target.closest('button'); if (!b) return; S.pct = +b.dataset.p; document.querySelectorAll('#pctSeg button').forEach(x => x.setAttribute('aria-pressed', x === b)); S.dirty = true; });

function renderCal(f) {
  const c = f.calibration; if (!c) return; const L = c.last, head = $('calHead'), body = $('calBody'), cb = $('calBadge'), ct = cb.lastElementChild;
  const left = c.age_s == null ? null : Math.max(0, c.ttl_s - c.age_s), mins = left == null ? '' : (left >= 60 ? Math.ceil(left / 60) + ' more min' : Math.round(left) + ' s');
  cb.style.opacity = 1; cb.className = 'badge';
  const tail = L ? ` Confidence ${L.confidence == null ? 'n/a' : Math.round(L.confidence * 100) + '%'} · z-separation ${L.z_sep} · ${c.passed} of ${c.runs} runs passed, ${c.streak} in a row.` : '';
  if (c.status === 'running') { head.textContent = 'Self-test running…'; body.textContent = c.phase || ''; cb.classList.add('warning'); ct.textContent = 'Self-test running…'; }
  else if (c.status === 'none') { head.textContent = 'Waiting for the first self-test…'; body.textContent = 'Runs automatically shortly after start, then every 150 s.'; ct.textContent = 'Not calibrated yet'; }
  else if (c.status === 'valid') { head.textContent = `Passed. We found the ${L.d_ms.toFixed(0)} ms where we hid it.`; body.textContent = L.reason + '.' + tail;
    cb.classList.add('good'); cb.style.opacity = 1 - 0.45 * Math.min(1, c.age_s / c.ttl_s); ct.textContent = `Calibrated ±${L.tol_ms} ms · valid ${mins}`; }
  else if (c.status === 'expired') { head.textContent = 'Calibration expired.'; body.textContent = `The last passing self-test is ${Math.round(c.age_s)} s old. Latency figures are unverified until the next one.`; cb.classList.add('warning'); ct.textContent = 'Calibration expired · unverified'; }
  else if (c.status === 'inconclusive') { head.textContent = 'Inconclusive: not enough samples.'; body.textContent = (L && L.reason) || ''; cb.classList.add('warning'); ct.textContent = 'Calibration inconclusive'; }
  else { head.textContent = L && L.found === null && /not sure/.test(L.reason || '') ? 'Failed. We were not sure where the delay was.' : 'Failed. We did not find the delay we hid.'; body.textContent = (L ? L.reason + (L.sham_ok ? '' : ' | sham: ' + L.sham_reason) + '.' : '') + ' Treat latency figures as unverified.'; cb.classList.add('critical'); ct.textContent = 'Calibration failed'; }
  $('calAgo').textContent = c.age_s == null ? '' : `last run ${Math.round(c.age_s)} s ago`; $('btnTest').disabled = c.status === 'running';
  $('induced').hidden = !f.induced;
  const sl = f.slo, t = $('tSlo'), unv = c.status !== 'valid';
  t.textContent = !sl || sl.met == null ? '—' : unv ? 'Unverified' : sl.met ? 'Met' : 'Breached'; t.style.color = unv && sl && sl.met != null ? 'var(--status-warning)' : sl && sl.met === false ? 'var(--status-critical)' : '';
  $('tSloS').textContent = sl && sl.p99_ms != null ? `pipeline p99 ${sl.p99_ms} ms vs ${sl.target_ms} · + ${sl.tick_wait_p50_ms} ms median wait for the next 10 Hz frame (by design) · ${sl.n} records${unv ? ' · calibration not valid' : ''}` : 'needs data';
}
$('btnTest').addEventListener('click', () => { $('btnTest').disabled = true; fetch('/calibrate', { method: 'POST' }).catch(() => {}); });
function renderDq(f) {
  const t = $('dq'); t.textContent = ''; const h = t.insertRow(); ['Venue', 'Empty seconds', 'Spread blank', 'Stalled for'].forEach(x => h.append(Object.assign(document.createElement('th'), { textContent: x })));
  for (const [k, s] of Object.entries(f.series)) { const d = s.dq; if (!d) continue; const r = t.insertRow(); r.insertCell().textContent = k; r.insertCell().textContent = `${d.empty_bins_60} of ${d.bins_60}`;
    r.insertCell().textContent = d.sentinel_rate_60 == null ? '—' : Math.round(d.sentinel_rate_60 * 100) + '% (no recent trade)'; r.insertCell().textContent = d.stall_s == null ? '—' : d.stall_s.toFixed(1) + ' s'; }
}

function renderTob(s) {
  const t = $('tob'); t.textContent = ''; const w = s && s.window_1s; if (!w) { t.append(el('div', 'empty', 'Waiting for data…')); return; }
  const mx = Math.max(w.best_bid_qty, w.best_ask_qty) || 1, mk = (cls, label, p, q) => { const d = el('div', 'side ' + cls); const i = document.createElement('i'); i.style.width = (q / mx * 100).toFixed(0) + '%';
    d.append(i, el('span', '', label), el('b', 'num', p.toFixed(2)), el('span', 'num', 'size ' + q.toFixed(4))); return d; };
  t.append(mk('bid', 'best bid', w.best_bid, w.best_bid_qty), mk('ask', 'best ask', w.best_ask, w.best_ask_qty));
  const tp = $('tape'); tp.textContent = ''; const tr = s.trades || [];
  $('tapeNote').textContent = tr.length ? 'Aggressor = the side that crossed the spread (derived from Fathom\'s is_buyer_maker; the same rule on both venues).' : 'No trades seen yet on this venue.';
  tr.slice().reverse().forEach(x => { const r = el('div', 'tr'); const d = new Date(x.t_ms); const pxs = el('span', 'px'); const dot = el('i', 'd'); dot.style.background = x.aggressor === 'buy' ? C.bid : C.ask;
    pxs.append(dot, document.createTextNode(`${x.px.toFixed(2)} ${x.aggressor === 'buy' ? '▲' : '▼'}`));
    r.append(el('span', '', d.toTimeString().slice(0, 8) + '.' + String(d.getMilliseconds()).padStart(3, '0')), pxs, el('span', 'q', x.qty.toFixed(4))); tp.append(r); });
}
function renderFeeds(f) {
  const t = $('feeds'); t.textContent = ''; const h = t.insertRow(); ['Venue', 'Updates/s', 'Last msg', 'State'].forEach(x => h.append(Object.assign(document.createElement('th'), { textContent: x })));
  for (const [k, s] of Object.entries(f.series)) { const w = s.window_1s, r = t.insertRow(); const cls = w ? ageClass(w.pulse_consume_age_ms)[1] : 'critical';
    const c0 = r.insertCell(); const d = el('i', 'sdot ' + (cls === 'good' ? '' : cls === 'warning' ? 'warning' : 'critical')); c0.append(d, document.createTextNode(k));
    r.insertCell().textContent = w ? w.n : '—'; r.insertCell().textContent = w ? `${w.pulse_consume_age_ms.toFixed(0)} ms ago` : 'no data'; r.insertCell().textContent = w ? ageClass(w.pulse_consume_age_ms)[2] : 'no data'; }
  $('sbConn').innerHTML = ''; for (const [k, s] of Object.entries(f.series)) { const w = s.window_1s; $('sbConn').append(el('span', '', `${k} ${w ? w.n : 0}/s   `)); }
}
const ICON = { info: 'ℹ', good: '✓', warning: '▲', serious: '■', critical: '✕' };
function renderLog() { S.logDirty = false; const l = $('log'); l.textContent = '';
  const all = [...S.log, ...S.srvEv].sort((x, y) => y.t_ms - x.t_ms).slice(0, 14); if (!all.length) { l.append(el('div', 'empty', 'Nothing has happened yet.')); return; }
  all.forEach(e => { const r = el('div', 'le'); r.append(el('span', '', new Date(e.t_ms).toTimeString().slice(0, 8)), el('span', 'ic', ICON[e.level] || 'ℹ'), el('span', '', e.text)); l.append(r); }); }

// ---------- render loop ----------
let lastStrip = 0;
function render() {
  const f = S.frame, t0 = performance.now(); if (!f) { renderChrome({ lag: {}, client: { drops: 0 } }, null, null); return; }
  const s = sel(), w = s && s.window_1s, now = f.ts_ms;
  renderChrome(f, s, w); drawMid(now); drawObi(now); drawTimeline(now); drawAnat(now); drawHist(); renderCal(f); renderDq(f); renderTob(s); renderFeeds(f);
  if (t0 - lastStrip > 900 || !$('strip').children.length) { renderStrip(f); lastStrip = t0; }
  if (S.logDirty || (f.events && f.events.length !== S.lastEv)) { S.lastEv = f.events ? f.events.length : 0; renderLog(); }
  $('sbSeq').textContent = `frame #${f.seq}` + (S.seqGaps ? ` · ${S.seqGaps} missed` : '');
  S.costs.push(performance.now() - t0); if (S.costs.length > 600) S.costs.shift(); S.draws++;
}
const pct = (a, p) => { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(p / 100 * s.length))]; };
let tick0 = performance.now();
function loop() { S.rafTicks++; if (S.dirty) { S.dirty = false; render(); } requestAnimationFrame(loop); }
setInterval(() => { $('sbClock').textContent = new Date().toTimeString().slice(0, 8) + ' local';
  const dt = (performance.now() - tick0) / 1000, fps = S.rafTicks / dt, p99 = pct(S.costs, 99);
  $('sbMeter').textContent = `render p99 ${p99 == null ? '—' : p99.toFixed(1)} ms · rAF ${fps.toFixed(0)} fps`;
  fetch('/debug/stats', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ raf_fps: +fps.toFixed(1), draws: S.draws, msgs: S.msgs, render_ms_p50: pct(S.costs, 50), render_ms_p99: p99,
    render_ms_max: S.costs.length ? Math.max(...S.costs) : null, seq_missed: S.seqGaps, dpr: window.devicePixelRatio, w: innerWidth, h: innerHeight, connected: S.connected, at: Date.now() }) }).catch(() => {});
  if (dt > 10) { S.rafTicks = 0; tick0 = performance.now(); } }, 1000);
fitAll(); logEv('info', 'Page loaded'); renderLog(); connect(); requestAnimationFrame(loop);
