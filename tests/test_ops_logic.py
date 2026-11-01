"""Ops-mode category logic (pulse/web/ops.js) runs under node: bands, hysteresis, smoothing, headline."""
import shutil, subprocess
from pathlib import Path

import pytest


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_ops_logic_js():
    r = subprocess.run(["node", str(Path(__file__).parent / "ops_logic_check.js")], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr + r.stdout
    assert "ops logic ok" in r.stdout
