"""GET /dashboard/summary – consolidated operational dashboard."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict
from pathlib import Path

from fastapi import APIRouter

from deltastack.broker.factory import get_broker, get_broker_status
from deltastack.config import get_settings
from deltastack.db.connection import get_db
from deltastack.db.dao_orders import list_errors, count_orders_today

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/summary")
def dashboard_summary():
    """Single consolidated JSON for operational monitoring."""
    settings = get_settings()
    db = get_db()

    # Data freshness
    from api.routers.freshness import _latest_mtime
    freshness = {
        "daily_bars": _latest_mtime(settings.bars_dir),
        "intraday_bars": _latest_mtime(settings.intraday_dir),
        "options_snapshots": _latest_mtime(settings.options_dir),
    }

    # Last orchestration
    orch_row = db.execute("SELECT * FROM orchestration_runs ORDER BY created_at DESC LIMIT 1").fetchall()
    last_orch = None
    if orch_row:
        cols = [d[0] for d in db.description]
        last_orch = {k: str(v) if v is not None else None for k, v in zip(cols, orch_row[0])}

    # Last ingest
    ingest_row = db.execute("SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 1").fetchall()
    last_ingest = None
    if ingest_row:
        cols = [d[0] for d in db.description]
        last_ingest = {k: str(v) if v is not None else None for k, v in zip(cols, ingest_row[0])}

    # Last signals batch
    sig_row = db.execute("SELECT MAX(created_at) as last_signal FROM signals").fetchone()
    last_signal = str(sig_row[0]) if sig_row and sig_row[0] else None

    # Positions & exposure
    try:
        broker = get_broker()
        positions = broker.get_positions()
        account = broker.get_account()
        pos_summary = [asdict(p) for p in positions]
        acct_summary = asdict(account)
    except Exception:
        pos_summary = []
        acct_summary = {}

    # Orders today
    orders_today = count_orders_today()

    # Last 10 errors
    recent_errors = list_errors(limit=10)

    return {
        "data_freshness": freshness,
        "last_orchestration": last_orch,
        "last_ingest": last_ingest,
        "last_signal_time": last_signal,
        "account": acct_summary,
        "positions": pos_summary,
        "orders_today": orders_today,
        "broker": get_broker_status(),
        "recent_errors": recent_errors,
    }
