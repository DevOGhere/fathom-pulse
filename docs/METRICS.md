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
- Stage 3 `pulse_consume → ws_send` and the browser segment **(Day 4)**. Browser segment is client-local and is never added into the absolute total.
- Calibration (self-test, confidence, expiry) **(Day 4)**. Until it passes, latency figures are unverified.
- Exchange-side latency: Binance book events carry `event_ts_ns = 0`, so the exchange stage is null there, never a fake zero.
- Sequence gaps, depth beyond best bid/ask, incident detection **(Day 6)**.

## Latency SLO (pipeline vs tick wait) — 2026-11-01

- **Stage 3 = consume -> ready for a frame** (record is in the aggregator). Exact: no tick quantisation. Stages 1-3 together are the **pipeline**; SLO: p99 < 100 ms over 60 s, induced (calibration) seconds excluded, from per-record totals (never a sum of per-stage p99s).
- **Tick wait** = `ready -> the next scheduled 10 Hz frame build` (0-100 ms). A display-cadence choice, not processing: reported beside the SLO (`tick_wait_p50/p99_ms`, per-bin `tw_*`) and never hidden inside it. **End to frame** = pipeline + tick wait (`end_to_frame_p99_ms`).
- Why the split: stage 3 used to be `consume -> frame build` and (a) read "Breached" structurally (p99 ~121 ms) because the tick wait alone is up to 100 ms, and (b) made calibration biased: Binance derived arrives every 100 ms, the same period as the tick, so arrival is phase-locked to the tick and delaying records by D shifts the tick wait unevenly. Stage-3 size errors were +4 to +10 ms (tolerance ~+-8); after the split they are about +-0.2 ms.
- First live reading (1416 records): pipeline p99 36.6 ms, tick wait p50 52 / p99 98 ms, end to frame p99 122.8 ms.
- **Calibration verdicts changed after seeing live failures (post-hoc, disclosed):** (1) detection needs delta >= `MIN_EFFECT_MS` (5 ms) as well as > 3 se: a 2.1 ms / 3.3 se upstream drift was a sham false alarm. (2) A sham that finds movement means the system was not steady (upstream stage 1 moved +11 to +16 ms during a quiet window, twice in ~10 min); that is reported **inconclusive** and retried after 20 s, not "failed". The gate reports every retry.
- Not measured / not claimed: network time from Pulse to the browser; Pulse is ms-class by design (Python, Kafka hop, 10 Hz cadence), not a µs system.

## pulse.fv.1 (`/features/latest`, `/features/window`) — 2026-11-01

Spec and rules live in the docstring of `pulse/api/features.py`; the field list is frozen by `tests/test_features.py` (GOLDEN). Closed 1 s bins only; latest and window share one shape; empty seconds and the effective-spread sentinel are null; `empty` and `induced` flags let a consumer drop them. Provenance: `derived_lag_ms_p50` is the median per-record pipeline latency of that second (stages 1-3, tick wait excluded), `pulse_age_ms` is `now - bin close` at request time. **Retention is the in-memory ring only (5 min / 300 bins), not the 30 min + ClickHouse in the original plan**: `/history` and the ClickHouse stitch are an extension. There is no model in Pulse; FathomFlow consumes this.
