# Seal checklist

Seal = tag `v1.0` locally once every item below holds. Nothing here pushes.

- [ ] `sh tools/seal_check.sh <scratch>` prints ALL GATES PASSED (record the date and commit)
- [ ] The page was looked at in a real browser: visuals OK, true 60 fps (not the headless ~37)
- [ ] QA findings for the build days were read (or consciously waived)
- [ ] `/debug/stats` removed or gated if anything is published
- [ ] Public-data policy confirmed: only crypto captures; capture size/format decided (17 MB raw per 90 s)
- [ ] README "What this is not" still true; METRICS.md caveats still true
- [ ] Real-fault check passes: `tools/chaos.py 8` -> stall causes say processor side, lag_spike opens, processor resumes (run it before recording the demo)
- [ ] Demo video recorded from `docs/DEMO.md`
- [ ] Only then: `git tag -a v1.0 -m "FathomPulse sealed"`; publishing (push, GitHub Pages) is a separate decision
