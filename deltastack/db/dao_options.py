"""DAO extensions for options backtests, execution plans, and events."""

from __future__ import annotations

import json
import uuid
import logging
from typing import List, Optional

import duckdb
from deltastack.db.connection import get_db

logger = logging.getLogger(__name__)


def _uid() -> str:
    return uuid.uuid4().hex[:16]


# ═══════════════════════════════════════════════════════════════════════════════
# options_backtest_runs
# ═══════════════════════════════════════════════════════════════════════════════

def insert_options_backtest_run(
    *,
    run_id: str,
    strategy: str,
    underlying: str,
    params: dict,
    metrics: dict,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> None:
    c = conn or get_db()
    c.execute(
        "INSERT INTO options_backtest_runs (run_id, strategy, underlying, params_json, metrics_json) VALUES (?,?,?,?,?)",
        [run_id, strategy, underlying, json.dumps(params), json.dumps(metrics)],
    )


def insert_options_trade(
    *,
    run_id: str,
    underlying: str,
    strategy: str,
    short_strike: float,
    long_strike: float,
    expiration: str,
    option_type: str,
    contracts: int,
    credit: float,
    max_loss: float,
    pnl: float,
    exit_reason: str = "",
    meta: Optional[dict] = None,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> str:
    c = conn or get_db()
    trade_id = _uid()
    c.execute(
        """INSERT INTO options_trades
           (trade_id, run_id, underlying, strategy, short_strike, long_strike,
            expiration, option_type, contracts, credit, max_loss, pnl, exit_reason, meta_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [trade_id, run_id, underlying, strategy, short_strike, long_strike,
         expiration, option_type, contracts, credit, max_loss, pnl, exit_reason,
         json.dumps(meta or {})],
    )
    return trade_id


def get_options_backtest_run(run_id: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> Optional[dict]:
    c = conn or get_db()
    rows = c.execute("SELECT * FROM options_backtest_runs WHERE run_id = ?", [run_id]).fetchall()
    if not rows:
        return None
    cols = [d[0] for d in c.description]
    return dict(zip(cols, rows[0]))


# ═══════════════════════════════════════════════════════════════════════════════
# execution_plans
# ═══════════════════════════════════════════════════════════════════════════════

def insert_execution_plan(
    *,
    plan_id: str,
    request_json: str,
    orders_json: str,
    risk_summary: str,
    status: str = "pending",
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> None:
    c = conn or get_db()
    c.execute(
        "INSERT INTO execution_plans (plan_id, request_json, orders_json, risk_summary, status) VALUES (?,?,?,?,?)",
        [plan_id, request_json, orders_json, risk_summary, status],
    )


def get_execution_plan(plan_id: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> Optional[dict]:
    c = conn or get_db()
    rows = c.execute("SELECT * FROM execution_plans WHERE plan_id = ?", [plan_id]).fetchall()
    if not rows:
        return None
    cols = [d[0] for d in c.description]
    return dict(zip(cols, rows[0]))


def update_plan_status(plan_id: str, status: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> None:
    c = conn or get_db()
    c.execute("UPDATE execution_plans SET status = ? WHERE plan_id = ?", [status, plan_id])


def insert_execution_event(
    *,
    plan_id: str,
    event_type: str,
    details: Optional[dict] = None,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> None:
    c = conn or get_db()
    c.execute(
        "INSERT INTO execution_events (plan_id, event_type, details_json) VALUES (?,?,?)",
        [plan_id, event_type, json.dumps(details or {})],
    )
