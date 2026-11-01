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

const nowMs = () => (S.replay ? (S.frame ? S.frame.ts_ms : S.replay.start_ms) : Date.now());   // replay: the log is stamped by recorded time, not by when you opened the page
function logEv(level, text) { S.log.unshift({ t_ms: nowMs(), level, text }); if (S.log.length > 40) S.log.pop(); S.logDirty = true; }
const LOCAL = ['localhost', '127.0.0.1', '[::1]'].includes(location.hostname);   // stage 4 is only meaningful on the same host clock

// ---------- socket ----------
const RQ = new URLSearchParams(location.search), REPLAY_URL = RQ.get('replay'), MANUAL = RQ.has('manual');
let ws, backoff = 500;
// ---------- replay: recorded frames through the SAME ingest()/render() path as live data ----------
async function startReplay(url) {
  const txt = await (await fetch(url)).text(), lines = txt.split('\n').filter(Boolean), head = JSON.parse(lines[0]).capture, frames = lines.slice(1).map(l => JSON.parse(l));
  S.replay = head; S.connected = true; S.everConnected = true;
  const feed = i => { const f = frames[i]; f.client = { drops: 0, sent: i + 1, queue_max: 2 }; S.lastSeq = 0; ingest(f); };
  const reset = () => { S.tail = {}; S.lagHist = []; S.lastSeq = 0; S.seqGaps = 0; S.log = []; S.sel = null; S.frame = null; S.pinned = null; };
  logEv('info', `Replaying a recording: ${head.title}`);
  window.__pulse = { head, n: frames.length,
    async run(n) { fitAll(); reset(); for (let i = 0; i < n; i++) feed(i); render(); S.dirty = false; return this.hash(); },     // deterministic jump for the replay test
    async hash() { const names = ['cMid', 'cObi', 'cQuote', 'cLag', 'cAnat', 'cHist', 'tLag', 'tRate', 'tSlo', 'tSloS', 'heroMid', 'calHead', 'dq', 'feeds', 'tape'];
      const parts = names.map(id => id.startsWith('c') && id !== 'calHead' ? $(id).toDataURL() : $(id).textContent), enc = new TextEncoder(),
        sha = async t => [...new Uint8Array(await crypto.subtle.digest('SHA-256', enc.encode(t)))].map(x => x.toString(16).padStart(2, '0')).join(''),
        each = await Promise.all(parts.map(sha)); return JSON.stringify({ all: await sha(each.join()), parts: Object.fromEntries(names.map((n, i) => [n, each[i].slice(0, 12)])), text: Object.fromEntries(names.slice(6).map((n, i) => [n, parts[i + 6].slice(0, 160)])) }); } };
  if (MANUAL) return;
  let i = 0; const t = setInterval(() => { if (i >= frames.length) { i = 0; reset(); logEv('info', 'Recording ended; starting again'); } feed(i++); }, 100);
}
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
  const x = c.getContext('2d', MANUAL ? { willReadFrequently: true } : undefined); x.setTransform(dpr, 0, 0, dpr, 0, 0); cvs[id] = { c, x, w, h };
}
const ids = ['cMid', 'cObi', 'cQuote', 'cLag', 'cAnat', 'cHist', 'eL', 'ePb', 'ePc', 'eO', 'eA', 'eT', 'eU', 'eB'];
function fitAll() { ids.forEach(fit); if (typeof OM !== 'undefined') OM.sec = -1; S.dirty = true; }
new ResizeObserver(fitAll).observe(document.body);

const PAD = { l: 64, r: 10, t: 6, b: 18 };
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
function bands(gg, now) {    // incident annotations: red = real, orange = induced on purpose
  for (const i of (S.frame.incidents || [])) { const t0 = Math.max(i.start_ms, now - TAIL_MS), t1 = Math.min(i.end_ms == null ? now : i.end_ms, now);
    if (t1 <= t0) continue; gg.x.fillStyle = i.induced ? 'rgba(236,131,90,0.16)' : 'rgba(229,72,77,0.20)'; gg.x.fillRect(px(gg, t0, now), PAD.t, Math.max(2, px(gg, t1, now) - px(gg, t0, now)), gg.h - PAD.t - PAD.b); }
}
function drawTimeline(now) {
  const s = sel(), g1 = cvs.cQuote, g2 = cvs.cLag;
  if (!s || !s.bins) { emptyChart(g1, 'Waiting for data…'); emptyChart(g2, 'Waiting for data…'); return; }
  const b = s.bins, rmax = Math.max(20, ...b.n_derived); axes(g1, 0, rmax, 2, v => v.toFixed(0) + '/s', false);
  bands(g1, now);
  const x = g1.x; x.strokeStyle = C.seq; x.lineWidth = 2; x.lineJoin = 'round'; x.beginPath();
  b.start_s.forEach((st, i) => { const X = px(g1, (st + 1) * 1000, now), Y = py(g1, b.n_derived[i], 0, rmax); i ? x.lineTo(X, Y) : x.moveTo(X, Y); }); x.stroke();
  const lh = S.lagHist.filter(p => p.lag != null); if (lh.length < 2) return emptyChart(g2, S.frame.lag.ok ? 'Waiting for lag samples…' : 'Lag unavailable: ' + (S.frame.lag.error || 'unknown'));
  const lmax = Math.max(50, ...lh.map(p => p.lag)); axes(g2, 0, lmax, 3, v => v.toFixed(0)); bands(g2, now);
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

const fmtAge = ms => ms < 1000 ? `${ms.toFixed(0)} ms` : ms < 60000 ? `${(ms / 1000).toFixed(1)} s` : `${(ms / 60000).toFixed(1)} min`;
function setChips(cls, glyph, text) { document.querySelectorAll('[data-chip]').forEach(ch => { ch.className = 'chip ' + cls; ch.textContent = `${glyph} ${text}`; }); }
function renderChrome(f, s, w) {
  const cb = $('connBadge'); cb.className = 'badge ' + (S.connected ? 'good' : 'critical'); $('connText').textContent = S.replay ? 'Replay · recorded, not live' : S.connected ? 'Live' : (S.everConnected ? 'Disconnected · numbers frozen' : 'Connecting…');
  if (w) { const [, cls, word, glyph] = ageClass(w.derived_age_ms); $('ageBadge').className = 'badge ' + cls; $('ageText').textContent = `${S.sel} · ${word} · ${w.derived_age_ms.toFixed(0)} ms old`; setChips(cls, glyph, `${w.derived_age_ms.toFixed(0)} ms · ${word}`); }
  else if (s && s.last_msg_age_ms != null) { const [, cls, word, glyph] = ageClass(s.last_msg_age_ms); $('ageBadge').className = 'badge ' + cls; $('ageText').textContent = `${S.sel} · ${word} · last update ${fmtAge(s.last_msg_age_ms)} ago`; setChips(cls, glyph, `${fmtAge(s.last_msg_age_ms)} · ${word}`); }
  else { $('ageBadge').className = 'badge'; $('ageText').textContent = 'no data yet'; setChips('', '○', 'no data'); }
  $('tLag').textContent = f.lag.total == null ? 'n/a' : f.lag.total; $('tLag').title = f.lag.ok ? '' : (f.lag.error || '');
  $('tRate').textContent = w ? w.n : (s ? 0 : '—'); $('tDrop').textContent = f.client.drops;
  if (w) { $('heroMid').textContent = w.mid.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    $('heroSub').textContent = `mid · ${QUOTE[S.sel.split('/')[0]] || '?'} · ${S.sel}`;
    $('spreadNote').textContent = `spread ${w.spread_abs.toFixed(2)} · ` + (w.effective_spread == null ? 'effective spread blank: no recent trade' : `effective spread ${w.effective_spread.toFixed(2)}`); }
}
const STAGE_COL = () => [css('--stage-recv'), css('--stage-derive'), css('--stage-pulse'), css('--stage-ws')];
const STAGE_NAME = ['1 · producer → derived (Fathom)', '2 · derived → Pulse consumed', '3 · consumed → ready for a frame', '4 · sent → your screen (local estimate)'];
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
  $('calAgo').textContent = c.age_s == null ? '' : `last run ${Math.round(c.age_s)} s ago`; $('btnTest').disabled = c.status === 'running' || !!S.replay;
  $('induced').hidden = !f.induced;
  const sl = f.slo, t = $('tSlo'), unv = c.status !== 'valid';
  t.textContent = !sl || sl.met == null ? '—' : unv ? 'Unverified' : sl.met ? 'Met' : 'Breached'; t.style.color = unv && sl && sl.met != null ? 'var(--status-warning)' : sl && sl.met === false ? 'var(--status-critical)' : '';
  $('tSloS').textContent = sl && sl.p99_ms != null ? `pipeline p99 ${sl.p99_ms} ms vs ${sl.target_ms} · + ${sl.tick_wait_p50_ms} ms median wait for the next 10 Hz frame (by design) · ${sl.n} records${unv ? ' · calibration not valid' : ''}` : 'needs data';
}
$('btnInduce').addEventListener('click', () => { $('btnInduce').disabled = true; fetch('/incident/induce?seconds=8', { method: 'POST' }).catch(() => {}); setTimeout(() => { $('btnInduce').disabled = !!S.replay; }, 9000); });
$('btnTest').addEventListener('click', () => { $('btnTest').disabled = true; fetch('/calibrate', { method: 'POST' }).catch(() => {}); });
const INC_NAME = { latency_drift: 'Pipeline got slower', stall: 'Feed stalled', rate_drop: 'Update rate collapsed', lag_spike: 'Processor falling behind' };
function renderIncidents(f) {
  const box = $('incs'), list = (f.incidents || []).slice().sort((a, b) => b.start_ms - a.start_ms); box.textContent = '';
  if (!list.length) { box.append(el('div', 'empty', 'No incidents in this session.')); return; }
  for (const i of list) {
    const d = el('div', 'inc'), h = el('div', 'h'), sev = i.severity === 'critical' ? 'critical' : i.severity === 'serious' ? 'serious' : 'warning';
    const dot = el('i', 'sdot ' + (i.end_ms == null ? 'critical' : 'warning')); if (i.end_ms != null) dot.style.background = 'var(--ink-muted)';
    h.append(dot, el('b', '', INC_NAME[i.kind] || i.kind), el('span', '', i.feed || 'processor'));
    if (i.induced) h.append(el('span', 'tag', 'induced'));
    const dur = i.end_ms == null ? 'ongoing' : `${((i.end_ms - i.start_ms) / 1000).toFixed(0)} s`, tm = new Date(i.start_ms).toTimeString().slice(0, 8);
    h.append(el('span', 't num', `${tm} · ${dur}`)); d.append(h, el('div', 'c', i.cause)); box.append(d);
  }
}
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
  for (const [k, s] of Object.entries(f.series)) { const w = s.window_1s, r = t.insertRow(); const lm = s.last_msg_age_ms, cls = w ? ageClass(w.pulse_consume_age_ms)[1] : lm != null ? ageClass(lm)[1] : 'critical';
    const c0 = r.insertCell(); const d = el('i', 'sdot ' + (cls === 'good' ? '' : cls === 'warning' ? 'warning' : 'critical')); c0.append(d, document.createTextNode(k));
    r.insertCell().textContent = w ? w.n : (lm != null ? 0 : '—'); r.insertCell().textContent = w ? `${w.pulse_consume_age_ms.toFixed(0)} ms ago` : lm != null ? `${fmtAge(lm)} ago` : 'no data'; r.insertCell().textContent = w ? ageClass(w.pulse_consume_age_ms)[2] : lm != null ? ageClass(lm)[2] : 'no data'; }
  $('sbConn').innerHTML = ''; for (const [k, s] of Object.entries(f.series)) { const w = s.window_1s; $('sbConn').append(el('span', '', `${k} ${w ? w.n : 0}/s   `)); }
}
const ICON = { info: 'ℹ', good: '✓', warning: '▲', serious: '■', critical: '✕' };
function renderLog() { S.logDirty = false; const l = $('log'); l.textContent = '';
  const all = [...S.log, ...S.srvEv].sort((x, y) => y.t_ms - x.t_ms).slice(0, 14); if (!all.length) { l.append(el('div', 'empty', 'Nothing has happened yet.')); return; }
  all.forEach(e => { const r = el('div', 'le'); r.append(el('span', '', new Date(e.t_ms).toTimeString().slice(0, 8)), el('span', 'ic', ICON[e.level] || 'ℹ'), el('span', '', e.text)); l.append(r); }); }

// ---------- render loop ----------
let lastStrip = 0;
// ---------- Ops dashboard: the same frames, 1 Hz, calmer encodings (area, heat strips, zoned band chart). See ops.js + docs/UX.md ----------
const OM = { mode: (RQ.get('mode') || localStorage.getItem('pulse.mode') || 'detail') === 'ops' ? 'ops' : 'detail', sec: -1, holds: {}, ema: { speed: new OPS.Ema(0.2), lag: new OPS.Ema(0.3) }, view: null, E: null, tips: {}, openInc: new Set(), draw: {}, hov: {}, cross: null };
const ICON_SEV = { good: '✔', warning: '▲', serious: '■', critical: '✕' };
const SEVC = { good: css('--status-good'), warning: css('--status-warning'), serious: css('--status-serious'), critical: css('--status-critical') };
const el2 = el;
function hold(key, name, v) { const h = OM.holds[key] || (OM.holds[key] = new OPS.Hold()), b = OPS.band(name, h.push(OPS.bandIndex(name, v))); b.settling = h.cand !== null; return b; }   // settling: the raw value already points elsewhere; the label waits for a few steady seconds
const roll = (a, k) => a.map((_, i) => { const w = a.slice(Math.max(0, i - k + 1), i + 1).filter(x => x != null); return w.length ? w.reduce((x, y) => x + y, 0) / w.length : null; });
const clock = sec => new Date(sec * 1000).toTimeString().slice(0, 8);

function buildE(f) {          // chart series from the frame's own 60 one-second bins (nothing synthetic)
  const V = [];
  for (const [k, s] of Object.entries(f.series)) { const b = s.bins; if (!b) continue; V.push({ k, tag: k.startsWith('binance') ? 'b' : 'c', start: b.start_s, mid: b.mid, obi: b.obi_top1, n: b.n_derived, trades: b.trade_count_1s, st: b.stages || {} }); }
  if (!V.length) return null;
  const ax = V[0].start, at = (v, arr, st) => { const i = v.start.indexOf(st); return i < 0 || !arr ? null : arr[i]; };
  const p50 = ax.map(st => { const x = V.map(v => at(v, v.st.tot_p50, st)).filter(y => y != null); return x.length ? x.reduce((a, c) => a + c, 0) / x.length : null; });
  const p99 = ax.map(st => { const x = V.map(v => at(v, v.st.tot_p99, st)).filter(y => y != null); return x.length ? Math.max(...x) : null; });
  const avg = key => { const x = V.flatMap(v => (v.st[key] || []).filter(y => y != null)); return x.length ? x.reduce((a, c) => a + c, 0) / x.length : null; };
  return { V, ax, p50: roll(p50, 5), p99: roll(p99, 5), p50raw: p50, p99raw: p99, share: { s1: avg('s1_p50'), s2: avg('s2_p50'), s3: avg('s3_p50'), tw: avg('tw_p50') } };
}
function opsSample(f) {          // one decision per frame-clock second: categories, smoothed numbers, headline
  const sevs = [], venues = {};
  for (const [k, s] of Object.entries(f.series)) {
    const w = s.window_1s, age = w ? w.derived_age_ms : (s.last_msg_age_ms != null ? s.last_msg_age_ms : 1e9);
    const fresh = hold(k + ':fresh', 'fresh', age), ratio = OPS.rateRatio(s.bins ? s.bins.n_derived : []), rate = ratio == null ? null : hold(k + ':rate', 'rate', ratio);
    const mids = s.bins ? s.bins.mid : [], lastMid = [...mids].reverse().find(x => x != null), firstMid = mids.find(x => x != null);
    venues[k] = { fresh, rate, px: lastMid, chg: lastMid != null && firstMid ? (lastMid / firstMid - 1) * 100 : null, mids, age, ratio };
    sevs.push(fresh.sev); if (rate) sevs.push(rate.sev);
  }
  const sl = f.slo, sp = sl && sl.p99_ms != null ? OM.ema.speed.push(sl.p99_ms) : null, speed = sp == null ? null : hold('speed', 'speed', sp);
  const lg = f.lag && f.lag.total != null ? OM.ema.lag.push(f.lag.total) : null, backlog = lg == null ? null : hold('backlog', 'backlog', lg);
  if (speed) sevs.push(speed.sev); if (backlog) sevs.push(backlog.sev);
  const inc = f.incidents || [], openReal = inc.some(i => i.end_ms == null && !i.induced), induced = !!f.induced || inc.some(i => i.end_ms == null && i.induced);
  const c = f.calibration || {}, trust = c.status === 'valid' ? { sev: 'good', word: 'Verified', note: 'Pulse checked its own timing and found a known delay.' }
    : c.status === 'running' ? { sev: 'warning', word: 'Checking', note: 'A self-test is running.' }
    : { sev: 'warning', word: 'Unverified', note: c.status === 'failed' ? 'The last self-test failed.' : 'The timing has not been verified recently.' };
  OM.view = { venues, speed, speedMs: sp, backlog, lagN: lg, trust, head: OPS.headline(sevs, openReal, induced), inc }; OM.E = buildE(f);
}

// tooltips: WCAG 1.4.13 (content on hover/focus): shows on hover AND keyboard focus, stays while the pointer is on it, Esc dismisses.
const TIP = { el: null, key: null, anchor: null, tIn: 0, tOut: 0 };
const tipTarget = n => (n && n.closest ? n.closest('[data-tip]') : null);
function tipFill() { const t = OM.tips[TIP.key]; if (!t) return; TIP.el.textContent = ''; TIP.el.append(el('div', 'th', t.h), el('div', 'tt', t.t)); }
function tipPlace() { const r = TIP.anchor.getBoundingClientRect(), tw = TIP.el.offsetWidth, th = TIP.el.offsetHeight;
  let x = Math.min(Math.max(8, r.left), innerWidth - tw - 8), y = r.bottom + 8; if (y + th > innerHeight - 8) y = Math.max(8, r.top - th - 8);
  TIP.el.style.left = x + 'px'; TIP.el.style.top = y + 'px'; }
function tipShow(a) { clearTimeout(TIP.tOut); TIP.anchor = a; TIP.key = a.dataset.tip; tipFill(); TIP.el.hidden = false; tipPlace(); a.setAttribute('aria-describedby', 'otip'); }
function tipHide() { clearTimeout(TIP.tIn); if (TIP.anchor) TIP.anchor.removeAttribute('aria-describedby'); TIP.el.hidden = true; TIP.anchor = null; TIP.key = null; }
function tipAt(x, y, tip) { OM.tips['chart'] = tip; TIP.anchor = { dataset: { tip: 'chart' }, getBoundingClientRect: () => ({ left: x + 14, right: x + 14, top: y, bottom: y + 14 }), setAttribute() {}, removeAttribute() {} }; TIP.key = 'chart'; clearTimeout(TIP.tOut); tipFill(); TIP.el.hidden = false; tipPlace(); }
const tipped = (node, key, tip) => { OM.tips[key] = tip; node.dataset.tip = key; node.tabIndex = 0; return node; };

// ---- chart geometry + drawing (canvases are sized by fit(); every draw takes the current crosshair index) ----
const G = { T: 12, B: 22 };
const px2 = (g, i, n, L, R) => L + (g.w - L - R) * (n <= 1 ? 1 : i / (n - 1));
function xLabels(g, L, R) { const x = g.x; x.fillStyle = C.muted; x.font = '11px ' + css('--font-ui'); x.textBaseline = 'alphabetic'; x.textAlign = 'left'; x.fillText('1 min ago', L, g.h - 6); x.textAlign = 'right'; x.fillText('now', g.w - R, g.h - 6); }
function cross(g, id, n, L, R, y0, y1) { const c = OM.cross; if (!c || c.id !== id) return null; const X = px2(g, c.i, n, L, R); g.x.strokeStyle = 'rgba(255,255,255,.35)'; g.x.lineWidth = 1; g.x.beginPath(); g.x.moveTo(Math.round(X) + .5, y0); g.x.lineTo(Math.round(X) + .5, y1); g.x.stroke(); return X; }
function drawArea(id, vals, raw, color) {
  const g = cvs[id]; if (!g) return; const x = g.x, L = 8, R = 8, n = vals.length, pts = vals.map((v, i) => [i, v]).filter(p => p[1] != null);
  x.clearRect(0, 0, g.w, g.h); if (pts.length < 3) return emptyChart(g, 'Collecting the first minute…');
  const lo = Math.min(...pts.map(p => p[1])), hi = Math.max(...pts.map(p => p[1])), pad = (hi - lo) * .18 || 1, a = lo - pad, b = hi + pad, Y = v => G.T + (g.h - G.T - G.B) * (1 - (v - a) / (b - a));
  const gr = x.createLinearGradient(0, G.T, 0, g.h - G.B); gr.addColorStop(0, color + '55'); gr.addColorStop(1, color + '00');
  x.beginPath(); pts.forEach((p, k) => (k ? x.lineTo(px2(g, p[0], n, L, R), Y(p[1])) : x.moveTo(px2(g, p[0], n, L, R), Y(p[1])))); x.lineTo(px2(g, pts[pts.length - 1][0], n, L, R), g.h - G.B); x.lineTo(px2(g, pts[0][0], n, L, R), g.h - G.B); x.closePath(); x.fillStyle = gr; x.fill();
  x.beginPath(); pts.forEach((p, k) => (k ? x.lineTo(px2(g, p[0], n, L, R), Y(p[1])) : x.moveTo(px2(g, p[0], n, L, R), Y(p[1])))); x.strokeStyle = color; x.lineWidth = 3; x.lineJoin = x.lineCap = 'round'; x.stroke();
  x.fillStyle = C.muted; x.font = '11px ' + css('--font-ui'); x.textAlign = 'right'; x.fillText(Math.round(hi).toLocaleString(), g.w - R, G.T + 10); x.fillText(Math.round(lo).toLocaleString(), g.w - R, g.h - G.B - 4);
  const X = cross(g, id, n, L, R, G.T, g.h - G.B), last = pts[pts.length - 1], ci = OM.cross && OM.cross.id === id ? OM.cross.i : last[0], cv = vals[ci];
  if (cv != null) { x.fillStyle = C.ring; x.beginPath(); x.arc(px2(g, ci, n, L, R), Y(cv), 7, 0, 7); x.fill(); x.fillStyle = color; x.beginPath(); x.arc(px2(g, ci, n, L, R), Y(cv), 4.5, 0, 7); x.fill(); }
  xLabels(g, L, R);
}
function drawLatency(E) {
  const g = cvs.eL; if (!g) return; if (!E) return emptyChart(g, 'Filling the first minute…'); const x = g.x, L = 8, R = 66, n = E.ax.length; x.clearRect(0, 0, g.w, g.h);
  const have = E.p99.filter(v => v != null); if (have.length < 3) return emptyChart(g, 'Collecting the first minute…');
  const ymax = Math.min(400, Math.max(150, Math.max(...have) * 1.2)), Y = v => G.T + (g.h - G.T - G.B) * (1 - Math.min(v, ymax) / ymax);
  const zones = [[0, 50, 'good', .10, 'Smooth'], [50, 100, 'good', .045, 'Normal'], [100, 200, 'warning', .10, 'Slow'], [200, 1e9, 'serious', .10, 'Late']];
  for (const [z0, z1, sev, al, name] of zones) { if (z0 >= ymax) continue; const y0 = Y(Math.min(z1, ymax)), y1 = Y(z0); x.fillStyle = SEVC[sev] + Math.round(al * 255).toString(16).padStart(2, '0'); x.fillRect(L, y0, g.w - L - R, y1 - y0);
    x.fillStyle = C.muted; x.font = '11px ' + css('--font-ui'); x.textAlign = 'left'; x.textBaseline = 'middle'; x.fillText(name, g.w - R + 8, (y0 + y1) / 2); if (z0) { x.fillText(z0 + ' ms', g.w - R + 8, y1 - 7); } }
  x.textBaseline = 'alphabetic';
  const idx = E.p99.map((v, i) => i).filter(i => E.p99[i] != null && E.p50[i] != null);
  x.beginPath(); idx.forEach((i, k) => (k ? x.lineTo(px2(g, i, n, L, R), Y(E.p99[i])) : x.moveTo(px2(g, i, n, L, R), Y(E.p99[i])))); [...idx].reverse().forEach(i => x.lineTo(px2(g, i, n, L, R), Y(E.p50[i]))); x.closePath(); x.fillStyle = C.seq + '40'; x.fill();
  x.beginPath(); idx.forEach((i, k) => (k ? x.lineTo(px2(g, i, n, L, R), Y(E.p99[i])) : x.moveTo(px2(g, i, n, L, R), Y(E.p99[i])))); x.strokeStyle = C.seq + '99'; x.lineWidth = 1.5; x.stroke();
  x.beginPath(); idx.forEach((i, k) => (k ? x.lineTo(px2(g, i, n, L, R), Y(E.p50[i])) : x.moveTo(px2(g, i, n, L, R), Y(E.p50[i])))); x.strokeStyle = C.seq; x.lineWidth = 3; x.lineJoin = x.lineCap = 'round'; x.stroke();
  cross(g, 'eL', n, L, R, G.T, g.h - G.B); xLabels(g, L, R);
}
function drawHeat(id, E, rows, extra) {              // rows: [{label, color(i) -> css|null}], 60 cells each
  const g = cvs[id]; if (!g) return; if (!E) return emptyChart(g, 'Filling the first minute…'); const x = g.x, L = 62, R = 8, n = E.ax.length, top = extra ? 12 : 4, rh = (g.h - top - G.B) / rows.length, cw = (g.w - L - R) / n; x.clearRect(0, 0, g.w, g.h);
  if (extra) extra(x, L, R, n, g);
  rows.forEach((r, ri) => { const y = top + ri * rh; x.fillStyle = C.sec; x.font = '12px ' + css('--font-ui'); x.textAlign = 'left'; x.textBaseline = 'middle'; x.fillText(r.label, 0, y + rh / 2 - 1);
    for (let i = 0; i < n; i++) { const c = r.color(i); x.fillStyle = c || 'rgba(255,255,255,.04)'; x.beginPath(); (x.roundRect || x.rect).call(x, L + i * cw + .5, y + 1, Math.max(1, cw - 1), rh - 3, 2); x.fill(); } });
  x.textBaseline = 'alphabetic'; cross(g, id, n, L, R, top - 2, g.h - G.B); xLabels(g, L, R);
}
function drawTrades(E) {
  const g = cvs.eT; if (!g) return; if (!E) return emptyChart(g, 'Filling the first minute…'); const x = g.x, L = 8, R = 8; x.clearRect(0, 0, g.w, g.h);
  const n = E.ax.length, cols = []; for (let s = 0; s + 5 <= n; s += 5) { let t = 0; for (const v of E.V) for (let j = s; j < s + 5; j++) t += v.trades && v.trades[j] != null ? v.trades[j] : 0; cols.push({ t, s }); }
  const max = Math.max(1, ...cols.map(c => c.t)), bw = (g.w - L - R) / cols.length, base = g.h - G.B; OM.tradeCols = cols;
  cols.forEach((c, i) => { const h = (base - G.T - 14) * c.t / max, X = L + i * bw + 3; x.fillStyle = OM.cross && OM.cross.id === 'eT' && OM.cross.i === i ? C.seq : C.seq + 'bb'; x.beginPath(); (x.roundRect || x.rect).call(x, X, base - h, Math.max(2, bw - 6), h, [4, 4, 0, 0]); x.fill(); });
  x.fillStyle = C.muted; x.font = '11px ' + css('--font-ui'); x.textAlign = 'right'; x.fillText(max + ' max', g.w - R, G.T + 4); xLabels(g, L, R);
}
function incMarks(E, inc) { return (x, L, R, n, g) => { const w = (g.w - L - R) / n; for (const i of inc) { const a = E.ax[0] * 1000, t0 = Math.max(0, (i.start_ms - a) / 1000), t1 = Math.min(n, ((i.end_ms == null ? E.ax[n - 1] * 1000 + 1000 : i.end_ms) - a) / 1000); if (t1 <= 0 || t0 >= n) continue;
  x.fillStyle = i.induced ? SEVC.serious : SEVC.critical; x.fillRect(L + t0 * w, 2, Math.max(3, (t1 - t0) * w), 5); } }; }

function opsDraw() {
  const E = OM.E, v = OM.view; if (!E) { ['eL', 'eO', 'eA', 'eT', 'ePb', 'ePc', 'eU', 'eB'].forEach(id => cvs[id] && emptyChart(cvs[id], 'Filling the first minute…')); return; } const inc = v.inc;
  for (const vn of E.V) { const id = 'eP' + vn.tag; OM.draw[id] = () => drawArea(id, roll(vn.mid, 3), vn.mid, vn.tag === 'b' ? C.seq : css('--seq-250')); }
  OM.draw.eL = () => drawLatency(E);
  const ratio = vn => { const nz = vn.n.filter(q => q > 0), base = OPS.median(nz); return i => (base == null || base < 3 ? null : (vn.n[i] || 0) / base); };
  OM.draw.eO = () => drawHeat('eO', E, E.V.map(vn => ({ label: OPS.VN(vn.k), color: i => { const o = vn.obi[i]; return o == null ? null : (o >= 0 ? C.bid : C.ask) + Math.round(Math.min(1, .12 + .88 * Math.abs(o)) * 255).toString(16).padStart(2, '0'); } })));
  OM.draw.eA = () => drawHeat('eA', E, E.V.map(vn => { const rr = ratio(vn); return { label: OPS.VN(vn.k), color: i => { const q = rr(i); if (q == null) return null; return q >= .7 ? SEVC.good : q >= .4 ? SEVC.warning : q > 0 ? SEVC.serious : '#7a1f1f'; } }; }), incMarks(E, inc));
  OM.draw.eT = () => drawTrades(E);
  const upd = E.ax.map((_, i) => E.V.reduce((a, vn) => a + (vn.n[i] || 0), 0)), end = E.ax[E.ax.length - 1] * 1000;
  const back = E.ax.map(st => { const h = S.lagHist.filter(q => q.lag != null && q.t >= st * 1000 && q.t < st * 1000 + 1000); return h.length ? h[h.length - 1].lag : null; });
  OM.draw.eU = () => drawArea('eU', roll(upd, 3), upd, css('--seq-250')); OM.draw.eB = () => (back.some(q => q) ? drawArea('eB', back, back, css('--seq-350')) : (cvs.eB && emptyChart(cvs.eB, 'Nothing waiting: the processor is keeping up')));
  Object.values(OM.draw).forEach(fn => fn());
  const R2 = id => ({ eL: 'eL' }[id] || id);
  // hover text per chart (index = second 0..59; rows by y)
  const t = i => clock(E.ax[Math.max(0, Math.min(E.ax.length - 1, i))]);
  for (const vn of E.V) OM.hov['eP' + vn.tag] = i => ({ h: `${OPS.VN(vn.k)} · ${t(i)}`, t: vn.mid[i] == null ? 'No update that second.' : `Mid price ${vn.mid[i].toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` });
  OM.hov.eL = i => { const a = E.p50raw[i], b = E.p99raw[i]; if (a == null) return { h: t(i), t: 'No updates that second.' }; const band = OPS.band('speed', OPS.bandIndex('speed', b)); return { h: t(i), t: `Typical ${OPS.fmtMs(a)}. Slowest 1 in 100: ${OPS.fmtMs(b)}, which reads ${band.word}.` }; };
  OM.hov.eO = (i, row) => { const vn = E.V[Math.max(0, Math.min(E.V.length - 1, row))], o = vn.obi[i]; return { h: `${OPS.VN(vn.k)} · ${t(i)}`, t: o == null ? 'No book update that second.' : o > .15 ? `Buyers lean: ${Math.round(o * 100)}% more resting buy size.` : o < -.15 ? `Sellers lean: ${Math.round(-o * 100)}% more resting sell size.` : 'Balanced.' }; };
  OM.hov.eA = (i, row) => { const vn = E.V[Math.max(0, Math.min(E.V.length - 1, row))], base = OPS.median(vn.n.filter(q => q > 0)); return { h: `${OPS.VN(vn.k)} · ${t(i)}`, t: `${vn.n[i] || 0} updates that second` + (base ? `; its normal is about ${Math.round(base)}.` : '.') }; };
  OM.hov.eT = i => { const c = OM.tradeCols && OM.tradeCols[Math.min(i, OM.tradeCols.length - 1)]; return c ? { h: `${t(c.s)} to ${t(Math.min(E.ax.length - 1, c.s + 4))}`, t: `${c.t} trades in 5 seconds, both venues.` } : null; };
}
const GEO = { eL: [8, 66], eO: [62, 8], eA: [62, 8], eT: [8, 8], ePb: [8, 8], ePc: [8, 8] };
function hoverInit() {
  const ops = $('ops');
  ops.addEventListener('mousemove', e => { const c = e.target.closest && e.target.closest('canvas'); if (!c || !OM.hov[c.id]) return; const g = cvs[c.id], r = c.getBoundingClientRect(), [L, R] = GEO[c.id], n = c.id === 'eT' ? (OM.tradeCols || []).length : 60;
    const fx = (e.clientX - r.left - L) / Math.max(1, g.w - L - R); if (fx < -.02 || fx > 1.02) return; const i = c.id === 'eT' ? Math.min(n - 1, Math.max(0, Math.floor(fx * n))) : Math.max(0, Math.min(59, Math.round(fx * 59)));
    const rows = c.id === 'eO' || c.id === 'eA' ? OM.E.V.length : 1, row = Math.floor((e.clientY - r.top - 4) / ((g.h - 4 - G.B) / rows)); const tip = OM.hov[c.id](c.id === 'eT' ? i : i, row);
    OM.cross = { id: c.id, i }; if (OM.draw[c.id]) OM.draw[c.id](); if (tip) tipAt(e.clientX, e.clientY, tip); });
  ops.addEventListener('mouseout', e => { const c = e.target.closest && e.target.closest('canvas'); if (c && OM.cross && OM.cross.id === c.id && !(e.relatedTarget && c.contains(e.relatedTarget))) { const id = c.id; OM.cross = null; if (OM.draw[id]) OM.draw[id](); tipHide(); } });
}
function initTips() { TIP.el = $('otip'); const ops = $('ops');
  const on = e => { const a = tipTarget(e.target); if (!a || a === TIP.anchor) return; clearTimeout(TIP.tIn); TIP.tIn = setTimeout(() => tipShow(a), e.type === 'focusin' ? 0 : 220); };
  const off = e => { clearTimeout(TIP.tIn); if (e.relatedTarget && TIP.el.contains(e.relatedTarget)) return; if (TIP.key !== 'chart') TIP.tOut = setTimeout(tipHide, 140); };
  ops.addEventListener('mouseover', on); ops.addEventListener('focusin', on); ops.addEventListener('mouseout', off); ops.addEventListener('focusout', off);
  TIP.el.addEventListener('mouseenter', () => clearTimeout(TIP.tOut)); TIP.el.addEventListener('mouseleave', () => { TIP.tOut = setTimeout(tipHide, 140); });
  document.addEventListener('keydown', e => { if (e.key === 'Escape' && !TIP.el.hidden) { tipHide(); if (OM.cross) { const id = OM.cross.id; OM.cross = null; OM.draw[id] && OM.draw[id](); } } });
  addEventListener('scroll', () => { if (!TIP.el.hidden) tipHide(); }, true); hoverInit(); }

function chip(b, key, tip) { const c = el2('span', 'chip2'); c.dataset.sev = b.sev; c.textContent = `${ICON_SEV[b.sev]} ${b.word}`; return tipped(c, key, tip); }
function tile(label, name, b, big, sub, key, tip) {          // KPI: bold figure, category word, gauge with the ranges VISIBLE under it
  const m = el2('div', 'm'); m.dataset.sev = b ? b.sev : 'good'; m.append(el2('div', 'lbl', label));
  const row = el2('div', 'bigrow'); row.append(el2('span', 'bigv', big || '—'), el2('b', 'word', b ? `${ICON_SEV[b.sev]} ${b.word}` : '')); m.append(row);
  if (name && b) { const bands = OPS.BANDS[name], seg = el2('div', 'seg'), lab = el2('div', 'bl'); seg.style.setProperty('--n', bands.length); lab.style.setProperty('--n', bands.length);
    bands.forEach((x, i) => { seg.append(el2('i', i <= b.i ? 'on' : '')); const t = el2('span', i === b.i ? 'cur' : '', x[2]); lab.append(t); }); m.append(seg, lab); }
  m.append(el2('span', 'dt', (sub || '') + (b && b.settling ? ' · label settling' : ''))); return b && tip ? tipped(m, key, tip) : m; }
function renderOps(f) {
  const sec = Math.floor(f.ts_ms / 1000); if (sec === OM.sec && OM.view) return; OM.sec = sec; opsSample(f);
  const v = OM.view, H = v.head, X = OPS.EXPLAIN, c = f.calibration || {}, E = OM.E;
  const parts = [...Object.entries(v.venues).map(([k, x]) => `${OPS.VN(k)}: ${x.fresh.word}` + (x.rate ? `, updates ${x.rate.word.toLowerCase()}` : '')), v.speed ? `Speed: ${v.speed.word}` : null, v.backlog ? `Backlog: ${v.backlog.word}` : null, `Trust: ${v.trust.word}`, v.inc.some(i => i.end_ms == null && !i.induced) ? 'Open incident: yes' : 'Open incidents: none'].filter(Boolean);
  const hero = $('opsHero'); hero.dataset.sev = H.sev; $('opsHead').textContent = `${ICON_SEV[H.sev]} ${H.word}`; $('opsSub').textContent = H.sub + (v.trust.word !== 'Verified' && H.word === 'All good' ? ` Timing is ${v.trust.word.toLowerCase()}.` : ''); tipped(hero, 'head', X.head(parts));
  const worst = Object.entries(v.venues).sort((a, b) => b[1].fresh.i - a[1].fresh.i || b[1].age - a[1].age)[0], K = $('opsMeters'); K.textContent = '';
  K.append(tile('Speed', 'speed', v.speed, v.speedMs == null ? '' : `${OPS.roundTo(v.speedMs, 10)} ms`, 'slowest 1 in 100, last minute', 'speed', v.speed ? X.speed(v.speed, v.speedMs) : null),
    tile('Backlog', 'backlog', v.backlog, v.lagN == null ? '' : `${OPS.roundTo(v.lagN, 10)}`, 'messages waiting for the processor', 'backlog', v.backlog ? X.backlog(v.backlog, v.lagN) : null),
    worst ? tile('Data age', 'fresh', worst[1].fresh, OPS.fmtMs(worst[1].age), `${OPS.VN(worst[0])}, the older of the two feeds`, 'fresh', X.fresh(worst[0], worst[1].fresh, worst[1].age)) : tile('Data age', null, null, '', ''));
  const TR = $('opsTrust'); TR.textContent = ''; TR.append(tile('Trust', null, v.trust, c.status === 'valid' && c.last ? `±${c.last.tol_ms} ms` : '', v.trust.note, 'trust', X.trust(c.status || 'none', c.age_s || 0, c.ttl_s || 180, c.last ? c.last.tol_ms : '?')));
  for (const [k, x] of Object.entries(v.venues)) { const t = k.startsWith('binance') ? 'b' : 'c'; if (!$('pn' + t)) continue; $('pn' + t).textContent = `${OPS.VN(k)} · ${k.split('/')[1].toUpperCase()}`;
    const pv = $('pv' + t); pv.textContent = x.px != null ? x.px.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '—'; if (x.chg != null) { const s = el2('small', '', `${x.chg >= 0 ? '▲' : '▼'} ${Math.abs(x.chg).toFixed(2)}%`); pv.append(s); } tipped(pv, 'px:' + k, X.price(k));
    const ch = $('ch' + t); ch.textContent = ''; ch.append(chip(x.fresh, 'f:' + k, X.fresh(k, x.fresh, x.age))); if (x.rate) ch.append(chip({ sev: x.rate.sev, word: 'Updates ' + x.rate.word.toLowerCase() }, 'r:' + k, X.rate(k, x.rate, x.ratio))); }
  document.querySelectorAll('h3[data-chart]').forEach(h => tipped(h, 'chart:' + h.dataset.chart, X.chart[h.dataset.chart]));
  const sh = $('opsShare'); sh.textContent = ''; if (E && E.share.s1 != null) { const S2 = E.share, segs = [['s1', 'Fathom', 'var(--stage-recv)'], ['s2', 'Hand-off to Pulse', 'var(--stage-derive)'], ['s3', 'Pulse', 'var(--stage-pulse)'], ['tw', 'Wait for next refresh', 'rgba(255,255,255,.22)']];
    const bar = el2('div', 'share'), list = el2('div', 'sl'); for (const [k, name, col] of segs) { const val = S2[k] == null ? 0 : S2[k], d = el2('div'); d.style.background = col; d.style.flex = `${Math.max(val, .4)} 1 0`; tipped(d, 'st:' + k, X.stage[k]); bar.append(d);
      const r = el2('div'); r.append(Object.assign(el2('span'), {})); r.firstChild.append(Object.assign(el2('i'), {}), document.createTextNode(name)); r.firstChild.firstChild.style.background = col; r.append(el2('b', '', OPS.fmtMs(val))); tipped(r, 'sl:' + k, X.stage[k]); list.append(r); } sh.append(bar, list); }
  const ib = $('opsIncs'); ib.textContent = ''; const list = v.inc.slice().sort((a, b) => b.start_ms - a.start_ms).slice(0, 5);
  if (!list.length) ib.append(el2('div', 'empty', 'Nothing has gone wrong in this session.')); else for (const i of list) { const d = el2('div', 'inc' + (OM.openInc.has(i.id) ? ' open' : '')), h = el2('div', 'h'); h.tabIndex = 0; h.setAttribute('role', 'button'); h.setAttribute('aria-expanded', OM.openInc.has(i.id));
    h.append(el2('span', 'car', '›'), el2('b', '', INC_NAME[i.kind] || i.kind), el2('span', '', i.feed ? OPS.VN(i.feed) : 'Processor')); if (i.induced) h.append(el2('span', 'tag', 'test'));
    h.append(el2('span', 't num', `${new Date(i.start_ms).toTimeString().slice(0, 8)} · ${i.end_ms == null ? 'ongoing' : OPS.fmtMs(i.end_ms - i.start_ms)}`));
    const flip = () => { OM.openInc.has(i.id) ? OM.openInc.delete(i.id) : OM.openInc.add(i.id); d.classList.toggle('open'); h.setAttribute('aria-expanded', OM.openInc.has(i.id)); };
    h.addEventListener('click', flip); h.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); flip(); } });
    d.append(h, el2('div', 'c', i.cause)); ib.append(d); }
  opsDraw(); if (TIP.el && !TIP.el.hidden && TIP.key !== 'chart') { tipFill(); tipPlace(); }
}
function applyMode(mode, persist) {
  const swap = () => {
    OM.mode = mode; OM.sec = -1; if (persist) localStorage.setItem('pulse.mode', mode);
    const ops = mode === 'ops'; $('ops').hidden = !ops; $('detail').hidden = ops; $('strip').hidden = ops; $('modeSwitch').dataset.mode = mode;
    $('swDetail').setAttribute('aria-checked', !ops); $('swOps').setAttribute('aria-checked', ops); $('swDetail').tabIndex = ops ? -1 : 0; $('swOps').tabIndex = ops ? 0 : -1;
    tipHide(); fitAll();                                // canvases measured 0 px while hidden: re-measure on every switch
    if (S.frame) { S.dirty = false; render(); }         // draw NOW: the transition snapshots the new view right after this callback returns
  };
  const calm = matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (document.startViewTransition && !calm && OM.ready && S.frame) document.startViewTransition(swap); else swap();
}
$('swDetail').addEventListener('click', () => applyMode('detail', true)); $('swOps').addEventListener('click', () => applyMode('ops', true));
$('modeSwitch').addEventListener('keydown', e => { if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') { const to = OM.mode === 'ops' ? 'detail' : 'ops'; applyMode(to, true); $(to === 'ops' ? 'swOps' : 'swDetail').focus(); e.preventDefault(); } });
function render() {
  if (OM.mode === 'ops' && S.frame) { const t0 = performance.now(); renderChrome(S.frame, sel(), sel() && sel().window_1s); renderCal(S.frame); renderOps(S.frame); S.costs.push(performance.now() - t0); if (S.costs.length > 600) S.costs.shift(); S.draws++; return; }
  const f = S.frame, t0 = performance.now(); if (!f) { renderChrome({ lag: {}, client: { drops: 0 } }, null, null); return; }
  const s = sel(), w = s && s.window_1s, now = f.ts_ms;
  renderChrome(f, s, w); drawMid(now); drawObi(now); drawTimeline(now); drawAnat(now); drawHist(); renderCal(f); renderIncidents(f); renderDq(f); renderTob(s); renderFeeds(f);
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
initTips(); applyMode(OM.mode, false); fitAll(); OM.ready = true; logEv('info', 'Page loaded'); renderLog(); if (REPLAY_URL) { startReplay(REPLAY_URL); $('btnTest').disabled = true; $('btnTest').title = 'Replays cannot run a self-test'; document.querySelector('.statusbar span:last-child').textContent = 'REPLAY · recorded data, not live'; } else connect(); requestAnimationFrame(loop);
