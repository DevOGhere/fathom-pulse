# FathomPulse — metric definitions and honesty rules (living doc)

Every number on screen has a definition here and a rule for what it is NOT. Items marked **(Day N)** are not built yet.

## Clocks (the rule everything else follows)
- `recv_ts_ns` (producer) and `derived_at_ns` (processor) are `CLOCK_MONOTONIC` on one host. `consume_mono_ns` is Pulse's monotonic stamp on the same host. Differences of these are the only valid stage durations.
- Wall-clock ages use Pulse's own `time.time_ns()`; a monotonic stamp is placed on the wall axis by a `ClockBridge` offset sampled every 5 s (median of 8 bracketed samples).
- Kafka `CreateTime` = **producer enqueue**, not broker append (cluster is `CreateTime`, verified 2026-10-29). Do not switch topics to `LogAppendTime`: it injects the Docker VM clock.
- A negative stage duration is **counted, not clamped** (`neg_stage` in every frame). Zero is expected; non-zero means the one-host assumption broke.

## Bins and KPIs (1 s closed bins, Pulse consume wall time)
- A bin `[s, s+1)` closes at `(s+1)s + 250 ms`; late messages are counted and dropped, a closed bin is never rewritten. Empty seconds emit an explicit empty bin.
- `quote_rate_1s` = derived updates in the bin. `trade_count_1s`, `avg_trade_size_60s` (total qty / trade count over trailing 60 bins; null when no trades).
- `realized_vol_60s` = sqrt(sum of squared 1 s log returns of bin-close mids) over the trailing 60 bins, non-empty bins only, null with < 2 returns. **Per-60 s, not annualised.**
- `effective_spread` arrives from Fathom as `0.0` meaning "no recent trade". Pulse maps it to **null at decode time**; the UI shows "blank: no recent trade", never `0`.
- Trade aggressor side: `is_buyer_maker = true` ⇒ the buyer was the maker ⇒ **the aggressor was the seller** (Binance `m` field; Coinbase side is the maker's side and Fathom's producer sets `is_buyer_maker = (side == "buy")`). Same rule on both venues.
- Quote currency: Binance is quoted in **USDT**, Coinbase in USD. Cross-venue comparisons must say "includes USDT/USD basis".

## Data-age chip
- **What it shows:** `derived_age_ms` = now − (processor `derived_at`, bridged to wall) for the newest derived update of the selected venue. It **includes the wait between updates**: Binance publishes every 100 ms, so about 50 ms of average age is structural, not lag.
- **Thresholds (calibrated 2026-10-29 from a 90 s capture):**

  | venue | p50 | p90 | p99 | max |
  |---|---|---|---|---|
  | binance/btcusdt | 53 ms | 154 ms | 354 ms | 552 ms |
  | coinbase/btcusd | 68 ms | 114 ms | 244 ms | 366 ms |

  fresh < 200 ms · aging < 500 ms · stale < 2 s · "not to be trusted" ≥ 2 s. Re-calibrate if the cadence or hosts change; a chip that is always amber carries no information.
- Every chip has an icon and a word, never colour alone. Status colours are reserved for state.

## Ages in the frame
| field | meaning |
|---|---|
| `derived_age_ms` | now − bridged `derived_at` of newest derived record |
| `pulse_consume_age_ms` | now − Pulse's consume wall stamp of newest record (staleness at Pulse) |
| `kafka_age_ms` | now − Kafka CreateTime (producer enqueue) |
| `pulse_age_ms` (frame) | staleness at Pulse across all venues (newest consume) |

## Not measured yet
- Stage 3 = `consume -> ready for a frame` (definition and why: see "Latency SLO" below; the wait for the next 10 Hz frame is reported separately). Stage 4 = server send -> your screen, a client-local estimate that exists only when the page runs on the same host clock; it is never added into the absolute total.
- Calibration (self-test, confidence, expiry) **(Day 4)**. Until it passes, latency figures are unverified. **Scope:** the self-test injects a delay at Pulse's own stamp sites (stages 2 and 3, consumer and aggregator) and checks it is found within ±N ms. It does not validate stage 1 (Fathom's own stamps), exchange clocks or network transit. The badge therefore reads "Self-test ±N ms", not "calibrated".
- Exchange-side latency: Binance book events carry `event_ts_ns = 0`, so the exchange stage is null there, never a fake zero.
- Sequence gaps, depth beyond best bid/ask, incident detection **(Day 6)**.

## Latency SLO (pipeline vs tick wait) — 2026-11-01

- **Stage 3 = consume -> ready for a frame** (record is in the aggregator). Exact: no tick quantisation. Stages 1-3 together are the **pipeline**; SLO: p99 < 100 ms over 60 s, induced (calibration) seconds excluded, from per-record totals (never a sum of per-stage p99s).
- **Tick wait** = `ready -> the next scheduled 10 Hz frame build` (0-100 ms). A display-cadence choice, not processing: reported beside the SLO (`tick_wait_p50/p99_ms`, per-bin `tw_*`) and never hidden inside it. **End to frame** = pipeline + tick wait (`end_to_frame_p99_ms`).
- Why the split: stage 3 used to be `consume -> frame build` and (a) read "Breached" structurally (p99 ~121 ms) because the tick wait alone is up to 100 ms, and (b) made calibration biased: Binance derived arrives every 100 ms, the same period as the tick, so arrival is phase-locked to the tick and delaying records by D shifts the tick wait unevenly. Stage-3 size errors were +4 to +10 ms (tolerance ~+-8); after the split they are about +-0.2 ms.
- First live reading (1416 records): pipeline p99 36.6 ms, tick wait p50 52 / p99 98 ms, end to frame p99 122.8 ms.
- **Prometheus cross-check rule changed after it failed (post-hoc, disclosed, 2026-11-03):** the old rule "Pulse recv->consume <= Fathom tick->store" assumed the store is slower than Pulse's Kafka hop. True with Fathom's default flags (~0.5 s), false with the sealed knobs (~8 ms, parallel path): Coinbase read 18.6 vs 17.2 ms. New rule: Pulse <= store + 5 ms (Pulse's documented stamping residual). Under the old defaults the check was close to vacuous (17 ms vs 500 ms); the new one compares two independent measurements to a few ms. Rate agreement (the other half) read +0.0% and -0.2%.
- **Calibration verdicts changed after seeing live failures (post-hoc, disclosed):** (1) detection needs delta >= `MIN_EFFECT_MS` (5 ms) as well as > 3 se: a 2.1 ms / 3.3 se upstream drift was a sham false alarm. (2) A sham that finds movement means the system was not steady (upstream stage 1 moved +11 to +16 ms during a quiet window, twice in ~10 min); that is reported **inconclusive** and retried after 20 s, not "failed". The gate reports every retry.
- Not measured / not claimed: network time from Pulse to the browser; Pulse is ms-class by design (Python, Kafka hop, 10 Hz cadence), not a µs system.

## pulse.fv.1 (`/features/latest`, `/features/window`) — 2026-11-01

Spec and rules live in the docstring of `pulse/api/features.py`; the field list is frozen by `tests/test_features.py` (GOLDEN). Closed 1 s bins only; latest and window share one shape; empty seconds and the effective-spread sentinel are null; `empty` and `induced` flags let a consumer drop them. Provenance: `derived_lag_ms_p50` is the median per-record pipeline latency of that second (stages 1-3, tick wait excluded), `pulse_age_ms` is `now - bin close` at request time. **Retention is the in-memory ring only (5 min / 300 bins), not the 30 min + ClickHouse in the original plan**: `/history` and the ClickHouse stitch are an extension. There is no model in Pulse; FathomFlow consumes this.

## Incidents (`pulse/api/incidents.py`) — 2026-11-01

Evaluated once a second on measured data; frames carry `incidents` (last 12 closed + all open); `GET /incidents`; shaded on the shared timeline (red = real, orange = induced).
- **stall**: no derived record for > 3 s per feed (clears under 1 s; critical past 10 s). **rate_drop**: 5 s mean rate is a robust outlier (median/MAD z < -4) and < 50% of the preceding 60 s median, judged only when that baseline is >= 3/s and never while a stall is open or within 6 s after one (its zeros belong to the stall). **lag_spike**: processor lag above max(200, median + 5 robust sigma) for 3 s in a row.
- **Cause text says only what is measured.** Silent feed + no processor backlog => "upstream of Fathom's processor (exchange feed, network or producer)", and whether one feed or both went quiet. With a backlog it says the cause is not clear from Pulse's side. It never names an exchange or a machine.
- **Induced demo**: `POST /incident/induce?seconds=N` (3-30) drops every record at ingest. Incidents overlapping it are labelled **induced**; the gate (`python -m pulse.api.incident_gate`) needs open <= 7 s, induced label, clear <= 10 s: first run 3/3 (opened 3.3-3.7 s, cleared 0.3-0.7 s).
- Not claimed: detection of a slowly rising latency (no drift rule yet), or anything about exchange-side causes. This session's real feed flapped (producer WebSocket keepalive timeouts), so real, un-induced incidents are expected on this machine.

## Ops view (slider: Detail / Ops) — 2026-11-01

Same frames, calmer presentation (`pulse/web/ops.js`, pure logic, tested under node). **Detail** is unchanged: every measurement at 10 Hz. **Ops** samples once per frame-clock second (so a replay matches), and shows words + icon + colour (never colour alone); exact figures stay in the hover text.
- **Freshness** (per feed): Fresh < 200 ms, Aging < 500, Stale < 2000, Not to be trusted beyond (the data-age chip bands). Uses last-update age during an outage.
- **Speed** (60 s pipeline p99, EMA-smoothed, shown rounded to 10 ms): Smooth < 50, Normal < 100, Slow < 200, Late beyond. **Backlog** (processor lag, smoothed): Keeping up < 50, Busy < 200, Behind beyond. **Updates** (5 s mean vs the feed's own earlier median, needs a healthy baseline): Normal >= 70%, Quiet >= 40%, Collapsed below.
- **Hysteresis:** a label gets worse only after 3 consecutive seconds beyond its band and better after 5, so one spike does not flip it and a flapping value does not flicker. Price shows the last *closed* second.
- **Headline** = the worst category; any open real incident forces at least "Degraded"; a deliberate test shows "Test in progress". Trust: Verified / Checking / Unverified from the calibration state.
- The thresholds are judgement calls named after the existing ones, not independently validated; tell me if a word feels wrong. Not claimed: that "All good" means the exchange data is correct, only that Pulse's own view is fresh and keeping up.

### Real-fault check (tools/chaos.py) and what it found — 2026-11-01

`tools/chaos.py 8` SIGSTOPs Fathom's host-side processor for 8 s (cap 10; SIGCONT on every exit path), so lag climbs and records stop for real, nothing synthetic. The first run showed two wrong behaviours that the induced-stall gate could not:
1. The stall's cause said "the processor has no backlog (78 msgs behind)... the gap is upstream" while the queue was climbing to 360. **Fix:** the cause now uses the queue's *trend*, not only its level (growing = above max(30, normal + 3 sigma) or +40 in 4 s), is rewritten while the stall is open as evidence arrives, and says "at or inside the processor, not at the exchange" when the queue grows. When it is flat it says "looks upstream of the processor... Pulse cannot tell which". It never names the exchange.
2. `lag_spike` did not open (lag was above 200 for only ~3 s at the 2 s poll). **Fix:** it also opens on a sustained climb: at least two successive *different* polls, each higher, at >= 100 and +80 over 4 s. One burst is two identical ticks and is not a climb.
3. A cosmetic one: after recovery the cause said "only one feed went quiet" because it was recomputed when both had recovered; it is now refreshed only while the feed is still silent.
Result on the real stall: both feeds' stalls (processor side, "both feeds went quiet together"), `lag_spike` (real, not induced), `latency_drift` (pipeline 18 -> 700 ms while the backlog drained). Peak lag 267 to 360.

## Inspector (Detail): click a bar = pause + open that second — 2026-11-02

Click a bar in *Anatomy of a Tick*: the charts freeze (frames keep arriving and are held; the badge says "Paused · inspecting"), and under the card the second opens: its own figures (updates, trades, mid, microprice, imbalance, self-test flag, each stage's P50/P99) plus **every record** from `GET /tick?venue&symbol&sec`: Pulse wall time, Kafka CreateTime, the four stage durations (monotonic differences), mid/microprice/imbalance/spread, and the record's **Kafka coordinates**: topic `market.derived.<venue>.<symbol>`, partition, offset, wire bytes, Confluent schema id, plus the raw producer and processor monotonic stamps. Esc or "Resume live" returns to live (the newest frame rebuilds the charts).
- Offsets are the message's real identity: a spot check against Redpanda (`rpk topic consume ... -o 932581`) returned the same `0:932581` and timestamp. Offsets within one second should be consecutive; a gap there means Pulse missed a message.
- There are no hashes: Pulse never computes any, so none are shown. Source is the in-memory ring (about the last 5 minutes); older seconds return nothing. In a replay (recording) only the second's own figures show. Detail mode only.
