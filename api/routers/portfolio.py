"""Portfolio analytics and reporting endpoints."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from typing import Optional

from fastapi import APIRouter, HTTPException

from deltastack.broker.factory import get_broker
from deltastack.db.dao import (
    get_backtest_run,
    get_trades_for_run,
    get_latest_positions,
)
from deltastack.db.connection import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/portfolio", tags=["portfolio"])


@router.get("/report")
def portfolio_report():
    """Return current portfolio summary: positions, P&L, exposure."""
    broker = get_broker()
    positions = broker.get_positions()
    account = broker.get_account()

    # Exposure by ticker
    exposure = {}
    total_delta = 0.0
    for p in positions:
        notional = p.qty * p.market_price
        exposure[p.ticker] = {
            "qty": p.qty,
            "market_price": p.market_price,
            "notional": round(notional, 2),
            "unrealized_pnl": p.unrealized_pnl,
        }

    # Realized P&L from DB (today + last 7 days)
    db = get_db()
    today_pnl_row = db.execute(
        "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE run_id = 'paper' AND entry_time >= CAST(current_date AS VARCHAR)"
    ).fetchone()
    today_pnl = float(today_pnl_row[0]) if today_pnl_row else 0.0

    week_pnl_row = db.execute(
        "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE run_id = 'paper' AND entry_time >= CAST(current_date - INTERVAL 7 DAY AS VARCHAR)"
    ).fetchone()
    week_pnl = float(week_pnl_row[0]) if week_pnl_row else 0.0

    unrealized = sum(p.unrealized_pnl for p in positions)

    return {
        "account": asdict(account),
        "positions": [asdict(p) for p in positions],
        "exposure_by_ticker": exposure,
        "pnl": {
            "unrealized": round(unrealized, 2),
            "realized_today": round(today_pnl, 2),
            "realized_7d": round(week_pnl, 2),
        },
        "num_positions": len(positions),
    }


@router.get("/backtest/{run_id}")
def backtest_report(run_id: str, include_curve: bool = False):
    """Return stored backtest run details, metrics, and optionally equity curve."""
    # Try equity backtest first
    run = get_backtest_run(run_id)
    if run is None:
        # Try options backtest
        from deltastack.db.dao_options import get_options_backtest_run
        run = get_options_backtest_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    # Parse JSON fields
    for key in ("params_json", "metrics_json"):
        if key in run and isinstance(run[key], str):
            try:
                run[key] = json.loads(run[key])
            except (json.JSONDecodeError, TypeError):
                pass

    # Serialize timestamps
    for key in ("created_at",):
        if key in run and run[key] is not None:
            run[key] = str(run[key])

    # Get associated trades
    trades = get_trades_for_run(run_id)
    for t in trades:
        if "meta_json" in t and isinstance(t["meta_json"], str):
            try:
                t["meta_json"] = json.loads(t["meta_json"])
            except (json.JSONDecodeError, TypeError):
                pass

    return {
        "run": run,
        "trades": trades,
        "trades_count": len(trades),
    }
