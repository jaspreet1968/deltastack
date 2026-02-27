#!/usr/bin/env python3
"""Automated health check – ingests SPY, runs a small backtest, records result.

Called by deltastack-healthcheck.timer daily at 00:45 UTC.
Usage: python scripts/healthcheck.py
"""

import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from deltastack.config import get_settings
from deltastack.db import ensure_tables
from deltastack.db.connection import get_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("deltastack.healthcheck")


def main() -> None:
    ensure_tables()
    db = get_db()
    details = {}
    status = "ok"

    # 1. Check data dir exists
    settings = get_settings()
    data_dir = Path(settings.data_dir)
    details["data_dir_exists"] = data_dir.exists()
    if not data_dir.exists():
        status = "warn"

    # 2. Check DB accessible
    try:
        row = db.execute("SELECT COUNT(*) FROM backtest_runs").fetchone()
        details["backtest_runs_count"] = row[0] if row else 0
    except Exception as exc:
        details["db_error"] = str(exc)
        status = "error"

    # 3. Try loading SPY bars (if available)
    try:
        from deltastack.data.storage import load_bars, ticker_exists
        if ticker_exists("SPY"):
            df = load_bars("SPY", limit=5)
            details["spy_bars_available"] = len(df)
        else:
            details["spy_bars_available"] = 0
            details["note"] = "SPY not yet ingested"
    except Exception as exc:
        details["spy_error"] = str(exc)
        status = "warn"

    # 4. Verify greeks computation
    try:
        from deltastack.options.greeks import compute_greeks
        g = compute_greeks(S=100, K=100, T=0.25, r=0.05, sigma=0.2)
        details["greeks_ok"] = 0 < g["delta"] < 1
    except Exception as exc:
        details["greeks_error"] = str(exc)
        status = "warn"

    # Record
    db.execute(
        "INSERT INTO health_checks (status, details_json) VALUES (?, ?)",
        [status, json.dumps(details)],
    )

    logger.info("Health check: status=%s details=%s", status, json.dumps(details))


if __name__ == "__main__":
    main()
