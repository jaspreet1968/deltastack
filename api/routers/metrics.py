"""GET /metrics/basic – lightweight in-memory observability counters."""

from __future__ import annotations

import logging
import time
from threading import Lock

from fastapi import APIRouter

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/metrics", tags=["metrics"])

# ── in-memory counters (reset on restart) ────────────────────────────────────
_start_time = time.monotonic()
_counters: dict = {
    "requests_total": 0,
    "ingest_requests": 0,
    "backtest_requests": 0,
    "trade_requests": 0,
    "last_ingest_time": None,
    "last_backtest_time": None,
}
_lock = Lock()


def increment(counter: str) -> None:
    """Thread-safe counter increment."""
    with _lock:
        _counters[counter] = _counters.get(counter, 0) + 1


def set_timestamp(key: str) -> None:
    """Record a timestamp for an event."""
    with _lock:
        _counters[key] = time.time()


@router.get("/basic")
def basic_metrics():
    """Return uptime, request counts, and last activity timestamps."""
    uptime_seconds = time.monotonic() - _start_time
    with _lock:
        snapshot = dict(_counters)

    snapshot["uptime_seconds"] = round(uptime_seconds, 1)
    snapshot["uptime_human"] = _format_uptime(uptime_seconds)
    return snapshot


def _format_uptime(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours}h {minutes}m {secs}s"
