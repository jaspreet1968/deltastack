#!/usr/bin/env python3
"""Capture QQQ intraday options chain snapshot.

Called by deltastack-options-snapshot-qqq.timer every 5 min during market hours.
Usage: python scripts/capture_options_snapshot.py [--underlying QQQ]
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from deltastack.config import get_settings
from deltastack.db import ensure_tables
from deltastack.db.connection import get_db
from deltastack.agent.runner import is_market_hours

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", stream=sys.stderr)
logger = logging.getLogger("deltastack.snapshot")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--underlying", default="QQQ")
    args = parser.parse_args()

    ensure_tables()

    if not is_market_hours():
        logger.info("Market closed – skipping snapshot capture")
        return

    settings = get_settings()
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(settings.market_timezone)
    except Exception:
        tz = None

    now = datetime.now(tz) if tz else datetime.utcnow()
    snap_date = now.date()
    snap_time = now.strftime("%H%M")

    logger.info("Capturing %s snapshot for %s %s", args.underlying, snap_date, snap_time)

    db = get_db()
    try:
        from deltastack.ingest.options_intraday import fetch_chain_snapshot_intraday
        result = fetch_chain_snapshot_intraday(args.underlying, snap_date, snap_time)
        rows = result.get("rows", 0)
        status = "ok" if rows > 0 else "empty"
        db.execute(
            "INSERT INTO options_snapshot_runs (underlying, snap_date, snap_time, status, rows_count) VALUES (?,?,?,?,?)",
            [args.underlying.upper(), str(snap_date), snap_time, status, rows],
        )
        logger.info("Snapshot captured: %d rows, status=%s", rows, status)
    except Exception as exc:
        db.execute(
            "INSERT INTO options_snapshot_runs (underlying, snap_date, snap_time, status, error_msg) VALUES (?,?,?,?,?)",
            [args.underlying.upper(), str(snap_date), snap_time, "error", str(exc)[:500]],
        )
        logger.exception("Snapshot capture failed")


if __name__ == "__main__":
    main()
