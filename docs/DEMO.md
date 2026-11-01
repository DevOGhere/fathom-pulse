# Demo video: script and shot list (about 3 minutes)

Record the live page at http://127.0.0.1:8765/ in a real browser (full screen, dark), with the producer/processor running. Every claim below maps to something the page really does. Do not say anything the page does not show.

**Before recording:** run `sh tools/seal_check.sh <scratch>` (all gates pass), restart Pulse for a clean session, wait ~90 s for baselines, close other tabs. Optional: `tools/capture.py 150 demo` so a replay is ready as a fallback.

| # | Shot | Say (about) |
|---|---|---|
| 1 | 0:00 Whole page, live | "Most dashboards pretend to be real-time. This one measures how late it is, and tests itself." |
| 2 | 0:15 Top badges + feed health | "Every number carries its age. Binance updates ten times a second, Coinbase about fifteen. This chip says how old the data on screen is." |
| 3 | 0:30 Anatomy of a Tick, click a bar to pin one second | "One update's life, split into measured stages: producer to derived, derived to Pulse, Pulse to frame. Real stamps, not estimates. Stage four, to your screen, is labelled as a local estimate." |
| 4 | 0:55 Latency distribution + SLO tile | "p99 pipeline latency against a 100 ms target. The wait for the next 10 Hz frame is shown separately, because it is a display choice, not processing." |
| 5 | 1:10 Calibration card, press "Run self-test now" | "Here is the idea I care about. Pulse hides an 80 ms delay inside one of its own stages and checks the waterfall finds the right stage and size. It found it, within about two milliseconds. A quiet run first proves it does not cry wolf." |
| 6 | 1:40 Calibration card states (point at expiry, "valid N more min") | "It expires. If it stops passing, or the system is not steady, the badge says so, instead of staying green." |
| 7 | 1:55 Press "Induce a stall" | "Now I break the feed on purpose." Watch the Incidents card + shaded timeline + event log. "Detected in about three seconds, labelled induced, with a cause that only says what Pulse can actually see." |
| 8 | 2:25 Market lens (mid, imbalance, top of book, tape) | "The market side: mid and microprice, order-book imbalance, top of book and trades. Aggressor side comes from Fathom's own buyer-maker flag." |
| 9 | 2:40 Replay: open `/?replay=/static/captures/demo.ndjson` | "Same page, same code path, from a recording. Two cold browsers render it byte-identically." |
| 10 | 2:55 Close on README "What this is not" | "It is ms-class by design. Not a microsecond system, and it says so." |

**Honest gaps to leave in, not cut:** the real feed flaps on this machine; if it flaps while recording, keep it, it is the best proof that an outage reads as "last update N s ago". Do not claim the sealed P99 63.4 ms for this rig (it runs default flags, 0.4 to 0.6 s tick-to-store).
