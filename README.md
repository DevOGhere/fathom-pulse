# FathomPulse

**The glass cockpit on [Fathom](https://github.com/DevOGhere) — a dashboard that measures its own truthfulness.**

One custom-designed screen, two lenses on the same timeline: **market microstructure** and the **engine room** (pipeline internals). Built on Fathom's live Kafka stream (Redpanda, ~100 raw msg/s, P99 tick-to-store 63.4ms).

## Thesis — "Honest Glass"

Everyone builds dashboards that pretend to be real-time. Pulse displays its own staleness.

1. **Latency provenance** — every panel carries a data-age chip; the signature "Anatomy of a Tick" panel decomposes it into provable stages (host-side monotonic stamps only; estimates are labeled as estimates, with error bars).
2. **Cross-lens causality** — market events and pipeline strain on the SAME timeline: volatility burst → message spike → consumer lag → latency shift.
3. **Lockstep flight-recorder replay** — both lenses replayed deterministically from a single capture file.

## Frame semantics (LOCKED 2026-07-03)

One WS frame = **a self-contained snapshot of the trailing 1-second window, emitted at 10Hz**.

- Every frame renders the entire UI on its own — no deltas, no client-side stitching.
- Backpressure drops (per-client queue, maxsize=2, drop-oldest) lose smoothness, never state. Drop counts are displayed in the engine lens.
- Every frame carries `pulse_age_ms` — the frame's own staleness.

## Layout

```
pulse/ingest   Kafka consumers (derived + raw trades), ring buffers, 1s closed bins
pulse/api      FastAPI: WS /stream, REST /history /infra /features (pulse.fv.1)
pulse/web      static frontend, no build step (uPlot + custom canvas)
pulse/web/ds   design-system component cards
captures/      flight-recorder capture files (JSONL, gitignored)
docs/          DESIGN.md, METRICS.md
```

## Run mode (non-negotiable)

Pulse runs **host-side**, like Fathom's producer/processor crib — monotonic timestamps are only comparable between processes on the same host kernel. The Fathom repo is read-only from here; Pulse never touches its compose, topics, or configs.
