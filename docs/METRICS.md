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
