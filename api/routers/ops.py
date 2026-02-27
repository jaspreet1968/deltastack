"""Operational status endpoints: /broker/status, /ops/status, /health/history."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from fastapi import APIRouter

from deltastack.broker.factory import get_broker_status
from deltastack.config import get_settings
from deltastack.db.connection import get_db
from deltastack.db.dao_orders import list_errors, count_orders_today

logger = logging.getLogger(__name__)
router = APIRouter(tags=["ops"])


# ── GET /broker/status ───────────────────────────────────────────────────────

@router.get("/broker/status")
def broker_status():
    """Return broker provider, mode, and paper URL validation."""
    return get_broker_status()


# ── GET /ops/status ──────────────────────────────────────────────────────────

@router.get("/ops/status")
def ops_status():
    """Comprehensive operational status for unattended monitoring."""
    settings = get_settings()
    db = get_db()

    # Last ingest run
    ingest_rows = db.execute(
        "SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 1"
    ).fetchall()
    last_ingest = None
    if ingest_rows:
        cols = [d[0] for d in db.description]
        last_ingest = {k: str(v) if v is not None else None for k, v in zip(cols, ingest_rows[0])}

    # Last signal
    signal_rows = db.execute(
        "SELECT * FROM signals ORDER BY created_at DESC LIMIT 1"
    ).fetchall()
    last_signal = None
    if signal_rows:
        cols = [d[0] for d in db.description]
        last_signal = {k: str(v) if v is not None else None for k, v in zip(cols, signal_rows[0])}

    # Last health check
    health_rows = db.execute(
        "SELECT * FROM health_checks ORDER BY checked_at DESC LIMIT 1"
    ).fetchall()
    last_health = None
    if health_rows:
        cols = [d[0] for d in db.description]
        last_health = {k: str(v) if v is not None else None for k, v in zip(cols, health_rows[0])}

    # Disk / DB
    db_path = Path(settings.resolved_db_path)
    db_size = db_path.stat().st_size if db_path.exists() else 0
    data_dir = Path(settings.data_dir)

    # Uptime + metrics
    from api.routers.metrics import _start_time, _counters
    uptime = time.monotonic() - _start_time

    # Error count (from middleware counters)
    from threading import Lock
    error_count = _counters.get("errors", 0)

    # Recent errors
    recent_errors = list_errors(limit=5)

    # Orders today
    orders_today = count_orders_today()

    # Data freshness (reuse freshness logic inline)
    from deltastack.data.cache import get_bars_cache
    bars_cache = get_bars_cache().stats()

    return {
        "uptime_seconds": round(uptime, 1),
        "broker": get_broker_status(),
        "last_ingest_run": last_ingest,
        "last_signal": last_signal,
        "last_health_check": last_health,
        "db_size_mb": round(db_size / 1_048_576, 2),
        "data_dir_exists": data_dir.exists(),
        "orders_today": orders_today,
        "recent_errors": recent_errors,
        "cache_stats": bars_cache,
        "request_counts": dict(_counters),
    }


# ── GET /ops/errors ──────────────────────────────────────────────────────────

@router.get("/ops/errors")
def ops_errors(limit: int = 50):
    """Return recent error log entries."""
    errors = list_errors(limit=limit)
    return {"errors": errors, "count": len(errors)}


# ── POST /ops/alert/test ─────────────────────────────────────────────────────

@router.post("/ops/alert/test")
def test_alert():
    """Send a test alert to the configured webhook URL."""
    from deltastack.alerts import send_alert
    sent = send_alert(
        title="DeltaStack Test Alert",
        message="This is a test alert from DeltaStack.",
        level="INFO",
        context={"test": True},
    )
    return {"sent": sent, "webhook_configured": bool(get_settings().alert_webhook_url)}


# ── GET /health/history ──────────────────────────────────────────────────────

@router.get("/health/history")
def health_history(limit: int = 20):
    """Return recent automated health check results."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM health_checks ORDER BY checked_at DESC LIMIT ?", [limit]
    ).fetchall()
    cols = [d[0] for d in db.description]
    checks = []
    for r in rows:
        row = dict(zip(cols, r))
        for k in ("checked_at",):
            if row.get(k):
                row[k] = str(row[k])
        if isinstance(row.get("details_json"), str):
            try:
                row["details_json"] = json.loads(row["details_json"])
            except (json.JSONDecodeError, TypeError):
                pass
        checks.append(row)
    return {"checks": checks}
