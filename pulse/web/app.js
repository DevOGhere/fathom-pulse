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
  lastSeq: 0, seqGaps: 0, lastDrops: 0, lagHigh: false, log: [], draws: 0, msgs: 0, costs: [], rafTicks: 0, lastStale: false };
const TAIL_MS = 60000, TAIL_MAX = 700;

function logEv(level, text) { S.log.unshift({ t: new Date().toTimeString().slice(0, 8), level, text }); if (S.log.length > 40) S.log.pop(); S.logDirty = true; }

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
const ids = ['cMid', 'cObi', 'cQuote', 'cLag'];
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
  $('tRate').textContent = w ? w.n : '—'; $('tDrop').textContent = f.client.drops; $('tAge').textContent = f.pulse_age_ms == null ? '—' : Math.round(f.pulse_age_ms);
  if (w) { $('heroMid').textContent = w.mid.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    $('heroSub').textContent = `mid · ${QUOTE[S.sel.split('/')[0]] || '?'} · ${S.sel}`;
    $('spreadNote').textContent = `spread ${w.spread_abs.toFixed(2)} · ` + (w.effective_spread == null ? 'effective spread blank: no recent trade' : `effective spread ${w.effective_spread.toFixed(2)}`); }
}
function renderAnat(w) {
  const b = $('anatBody'); b.textContent = ''; if (!w) { b.append(el('span', 'empty', 'Waiting for data…')); return; }
  const rows = [['1 · producer receive → processor derived', w.stage_recv_to_derived_ms], ['2 · processor derived → Pulse consumed', w.stage_derived_to_consume_ms]];
  const t = document.createElement('table'); t.className = 'feeds'; const h = t.insertRow(); ['Stage (host-side monotonic)', 'P50 ms', 'P99 ms'].forEach(x => h.append(Object.assign(document.createElement('th'), { textContent: x })));
  rows.forEach(([n, v]) => { const r = t.insertRow(); [n, v.p50, v.p99].forEach((x, i) => { const c = r.insertCell(); c.textContent = i ? (x == null ? '—' : x.toFixed(1)) : x; }); });
  b.append(t); b.append(el('div', 'empty', w.neg_stage ? `⚠ ${w.neg_stage} negative stage duration(s): clock-domain assumption broken` : 'No negative stage durations: one clock domain, as designed.'));
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
function renderLog() { S.logDirty = false; const l = $('log'); l.textContent = ''; if (!S.log.length) { l.append(el('div', 'empty', 'Nothing has happened yet.')); return; }
  S.log.slice(0, 12).forEach(e => { const r = el('div', 'le'); r.append(el('span', '', e.t), el('span', 'ic', ICON[e.level]), el('span', '', e.text)); l.append(r); }); }

// ---------- render loop ----------
let lastStrip = 0;
function render() {
  const f = S.frame, t0 = performance.now(); if (!f) { renderChrome({ lag: {}, client: { drops: 0 } }, null, null); return; }
  const s = sel(), w = s && s.window_1s, now = f.ts_ms;
  renderChrome(f, s, w); drawMid(now); drawObi(now); drawTimeline(now); renderAnat(w); renderTob(s); renderFeeds(f);
  if (t0 - lastStrip > 900 || !$('strip').children.length) { renderStrip(f); lastStrip = t0; }
  if (S.logDirty) renderLog();
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
