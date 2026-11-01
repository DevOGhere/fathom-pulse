#!/bin/sh
# Run every gate in order and print one verdict. Needs: Fathom infra + host producer/processor + `python -m pulse.api` running.
# Usage: sh tools/seal_check.sh <scratch_dir>      (takes ~15 min; a failing gate does not stop the others)
S="${1:?scratch dir for the throwaway Chrome profiles}"; cd "$(dirname "$0")/.." || exit 1; PY=.venv/bin/python; R=""; FAIL=0
step() { name="$1"; shift; echo "=== $name"; if "$@" > "$S/seal_$(echo "$name" | tr ' /' '__').log" 2>&1; then R="$R\n  PASS  $name"; else R="$R\n  FAIL  $name   (log: $S/seal_$(echo "$name" | tr ' /' '__').log)"; FAIL=1; fi; }
curl -sf -m 3 http://127.0.0.1:8765/health >/dev/null || { echo "Pulse is not running on :8765"; exit 2; }
step "unit + contract tests"      $PY -m pytest -q
step "ws soak 120s"               $PY -m pulse.api.soak --seconds 120 --slow-ms 500
step "calibration gate x10"       $PY -m pulse.api.calib_gate --runs 10
step "incident gate x3"           $PY -m pulse.api.incident_gate --runs 3 --seconds 8
step "prometheus cross-check"     $PY tools/sane_check.py
$PY tools/capture.py 60 seal-check --title "seal check capture" > "$S/seal_capture.log" 2>&1
step "replay determinism"         $PY tools/replay_check.py "$S" /static/captures/seal-check.ndjson 100,300,590
echo; echo "SEAL CHECK:$R"; [ $FAIL -eq 0 ] && echo "ALL GATES PASSED" || echo "AT LEAST ONE GATE FAILED"; exit $FAIL
