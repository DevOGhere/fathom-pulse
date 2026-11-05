# Seal checklist

Seal = tag `v1.0` once every item below holds. Nothing here pushes.

- [x] `sh tools/seal_check.sh <scratch>` prints ALL GATES PASSED (record the date and commit)
- [x] The page was looked at in a real browser: visuals OK, true 60 fps (not the headless ~37)
- [x] QA findings for the build days were read (or consciously waived)
- [x] `/debug/stats` and the test endpoints are removed or gated if anything is published (`PULSE_PUBLIC=1`)
- [x] Public-data policy confirmed: only crypto captures; capture size and format decided (17 MB raw per 90 s)
- [x] README "What this is not" still true; METRICS.md caveats still true
- [x] Real-fault check passes: `tools/chaos.py 8` -> stall causes say processor side, lag_spike opens, processor resumes (run it before recording the demo)
- [x] Demo video recorded from `docs/DEMO.md`
- [x] Only then: `git tag -a v1.0 -m "FathomPulse sealed"`; publishing (push, hosted replay) is a separate decision
