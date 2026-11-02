"""Hardening contract (security review): cross-site POSTs refused, public build hides test/debug endpoints, security headers,
client cap, and the once-per-tick frame serialiser is byte-identical to the per-client json.dumps it replaced.
Calls the handlers directly (no TestClient: it would add a dependency)."""
import asyncio
import inspect
import json

import pytest
from fastapi import HTTPException
from fastapi.responses import Response
from starlette.requests import Request

from pulse.api import app as appmod


def req(origin=None, host="127.0.0.1:8765"):
    h = [(b"host", host.encode())] + ([(b"origin", origin.encode())] if origin else [])
    return Request({"type": "http", "method": "POST", "path": "/x", "headers": h, "query_string": b""})


def status(fn, *a):
    try:
        asyncio.run(fn(*a)) if inspect.iscoroutinefunction(fn) else fn(*a)
        return 200
    except HTTPException as e:
        return e.status_code


def test_cross_origin_post_refused():
    assert status(appmod._guard_mutation, req("http://evil.example")) == 403
    assert status(appmod._guard_mutation, req("http://127.0.0.1:9999")) == 403          # same host, different port = different origin


def test_same_origin_or_no_origin_passes_the_guard():
    assert status(appmod._guard_mutation, req("http://127.0.0.1:8765")) == 200          # the page itself
    assert status(appmod._guard_mutation, req(None)) == 200                              # curl / our own tools


def test_public_build_hides_endpoints(monkeypatch):
    monkeypatch.setenv("PULSE_PUBLIC", "1")
    assert status(appmod._guard_mutation, req(None)) == 404
    assert status(appmod.get_stats) == 404


def test_debug_stats_available_locally(monkeypatch):
    monkeypatch.delenv("PULSE_PUBLIC", raising=False)
    assert status(appmod.get_stats) == 200


def test_security_headers():
    async def nxt(_):
        return Response("ok")
    r = asyncio.run(appmod.no_stale_assets(Request({"type": "http", "method": "GET", "path": "/", "headers": [], "query_string": b""}), nxt))
    assert r.headers["x-frame-options"] == "DENY" and r.headers["x-content-type-options"] == "nosniff"
    assert r.headers["referrer-policy"] == "no-referrer" and r.headers["cache-control"] == "no-cache"


def test_frame_text_matches_per_client_dumps():
    frame = {"v": 1, "seq": 7, "series": {"binance/btcusdt": {"a": [1.5, None, "x"]}}, "slo": {"ok": True}}
    blk = {"drops": 0, "sent": 3, "queue_max": 2, "sent_ms": 1.5e12}
    assert appmod.frame_text(frame, blk) == json.dumps({**frame, "client": blk}, separators=(",", ":"))
    blk2 = {**blk, "sent": 4}
    assert appmod.frame_text(frame, blk2) == json.dumps({**frame, "client": blk2}, separators=(",", ":"))   # cached frame part, new client part
    other = {**frame, "seq": 8}
    assert appmod.frame_text(other, blk) == json.dumps({**other, "client": blk}, separators=(",", ":"))     # a new frame replaces the cache


def test_max_clients_env(monkeypatch):
    monkeypatch.setenv("PULSE_MAX_CLIENTS", "3")
    assert appmod._max_clients() == 3
