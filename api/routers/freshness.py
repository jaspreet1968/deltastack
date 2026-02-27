"""GET /data/freshness – last updated timestamps for all data types."""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter

from deltastack.config import get_settings
from deltastack.db.connection import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/data", tags=["data"])


def _latest_mtime(directory: Path, pattern: str = "*.parquet") -> str | None:
    """Find the most recently modified file matching pattern."""
    if not directory.exists():
        return None
    latest = None
    for f in directory.rglob(pattern):
        mt = f.stat().st_mtime
        if latest is None or mt > latest:
            latest = mt
    if latest:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(latest, tz=timezone.utc).isoformat()
    return None


@router.get("/freshness")
def data_freshness():
    """Return last-updated timestamps for all data types."""
    settings = get_settings()
    db = get_db()

    # Daily bars
    daily_latest = _latest_mtime(settings.bars_dir)

    # Intraday bars
    intraday_latest = _latest_mtime(settings.intraday_dir)

    # Options snapshots
    options_latest = _latest_mtime(settings.options_dir)

    # Latest signal from DB
    sig_row = db.execute("SELECT MAX(created_at) FROM signals").fetchone()
    signals_latest = str(sig_row[0]) if sig_row and sig_row[0] else None

    # Latest ingest run
    ingest_row = db.execute("SELECT MAX(started_at) FROM ingestion_runs").fetchone()
    ingest_latest = str(ingest_row[0]) if ingest_row and ingest_row[0] else None

    return {
        "daily_bars_last_updated": daily_latest,
        "intraday_bars_last_updated": intraday_latest,
        "options_snapshots_last_updated": options_latest,
        "signals_last_generated": signals_latest,
        "ingest_last_run": ingest_latest,
    }
