# Design decisions

Each entry: what was decided, why, and what it cost. Dates are 2026.

**Frame = self-contained trailing-1 s snapshot at 10 Hz (locked Jul 3).** Any dropped frame loses smoothness, never state; a reconnecting client rebuilds everything from one frame. Cost: ~19 KB/frame (17 MB per 90 s capture), so hosted replays need compression or a smaller format.

**Closed 1 s bins, grace 250 ms, empty bins emitted, late records dropped and counted.** Closed means final: nothing ever rewrites a bin (train/serve parity for /features). Cost: up to 1.25 s before a second appears as a bin.

**Per-message stamps (Nov 1, after QA).** One monotonic + wall pair per record, at unbatch time, with `consume()` timeout 5 ms. A 50 ms batch timeout had added ~25 ms of Pulse's own wait to stage 2. Residual error: <= 5 ms of librdkafka queueing.

**Calibrated honesty (the one big idea).** Inject a delay at a real stamp site, with the truth measured (the delay actually applied, not the nominal one), then judge with noise-derived tolerance. Lessons that shaped it: (1) the first gate failed honestly (nominal truth) and the fix was the measurement, not the threshold; (2) stage 3 used to include the 10 Hz tick wait, and Binance's own 100 ms cadence is phase-locked to that tick, so delays shifted unevenly (+4..+10 ms bias): stage 3 is now consume -> ready, and tick wait is separate (error ~0.1 ms); (3) a quiet window in which something moved is *inconclusive, retry*, not a failure (upstream stage 1 really jumps 5-16 ms); (4) a 5 ms minimum effect was added after seeing a 2 ms false alarm. Verdict rules (3) and (4) were changed after seeing failures, so the later passes are not blind; the history is in METRICS.md.

**SLO = pipeline, tick wait reported beside it.** The cadence is a display choice, not processing. Live: pipeline p99 ~30-50 ms vs target 100; end-to-frame p99 ~120 ms.

**Incidents say what they see.** Three rules with hysteresis and cooldown; cause text is generated from measured evidence (silence + no processor backlog => "upstream of Fathom's processor"; with backlog => "not clear from Pulse's side"). Induced incidents carry a label and never read as outages. No drift rule yet.

**Replay reuses the live path.** Recorded frames go through the same `ingest()`/`render()`; the page draws from the frame's own timestamp, never `Date.now()`. Determinism is checked by hashing six canvases + key text in two cold browsers.

**Restrained glass (Oct 29).** Glass only on chrome; data surfaces near-opaque; the loud "liquid" variant was rejected. The mock is kept as a design reference (`pulse/web/ds/mock-v0.html`, synthetic).

**Fathom is read-only.** No edits to its repo, compose, topics or configs; Pulse reads Prometheus and Kafka only. The consumer group is unique per process (`pulse-ingest-<pid>`): a shared id split partitions between two instances and each silently saw half the stream.

**Shelved on purpose:** auditor replay export, two-feed cross-check, guided tour, alert gating, OTel through Kafka headers, ClickHouse-backed history (fv.1 is ring-only), any model inside Pulse (that is FathomFlow, a separate project).
