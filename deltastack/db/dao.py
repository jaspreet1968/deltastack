"""Data Access Objects for DuckDB tables.

All functions accept an optional ``conn`` parameter so callers can share a
transaction.  If omitted, the singleton connection from ``get_db()`` is used.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import duckdb

from deltastack.db.connection import get_db

logger = logging.getLogger(__name__)


def _uid() -> str:
    return uuid.uuid4().hex[:16]


# ═══════════════════════════════════════════════════════════════════════════════
# backtest_runs
# ═══════════════════════════════════════════════════════════════════════════════

def insert_backtest_run(
    *,
    run_id: str,
    strategy: str,
    tickers: str,
    params: dict,
    dt_start: str,
    dt_end: str,
    metrics: dict,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> None:
    c = conn or get_db()
    c.execute(
        """
        INSERT INTO backtest_runs (run_id, strategy, tickers, params_json, dt_start, dt_end, metrics_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [run_id, strategy, tickers, json.dumps(params), dt_start, dt_end, json.dumps(metrics)],
    )


def get_backtest_run(run_id: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> Optional[dict]:
    c = conn or get_db()
    rows = c.execute("SELECT * FROM backtest_runs WHERE run_id = ?", [run_id]).fetchall()
    if not rows:
        return None
    cols = [d[0] for d in c.description]
    return dict(zip(cols, rows[0]))


def list_backtest_runs(limit: int = 50, conn: Optional[duckdb.DuckDBPyConnection] = None) -> List[dict]:
    c = conn or get_db()
    rows = c.execute("SELECT * FROM backtest_runs ORDER BY created_at DESC LIMIT ?", [limit]).fetchall()
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# trades
# ═══════════════════════════════════════════════════════════════════════════════

def insert_trade(
    *,
    run_id: Optional[str] = None,
    ticker: str,
    side: str,
    qty: float = 0,
    entry_time: str = "",
    entry_price: float = 0,
    exit_time: str = "",
    exit_price: float = 0,
    pnl: float = 0,
    meta: Optional[dict] = None,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> str:
    c = conn or get_db()
    trade_id = _uid()
    c.execute(
        """
        INSERT INTO trades (trade_id, run_id, ticker, side, qty, entry_time, entry_price,
                            exit_time, exit_price, pnl, meta_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [trade_id, run_id, ticker, side, qty, entry_time, entry_price,
         exit_time, exit_price, pnl, json.dumps(meta or {})],
    )
    return trade_id


def get_trades_for_run(run_id: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> List[dict]:
    c = conn or get_db()
    rows = c.execute("SELECT * FROM trades WHERE run_id = ? ORDER BY entry_time", [run_id]).fetchall()
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# positions
# ═══════════════════════════════════════════════════════════════════════════════

def upsert_position(
    *,
    ticker: str,
    qty: float,
    avg_price: float,
    unrealized_pnl: float = 0,
    meta: Optional[dict] = None,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> None:
    c = conn or get_db()
    c.execute(
        """
        INSERT INTO positions (ticker, qty, avg_price, unrealized_pnl, meta_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        [ticker, qty, avg_price, unrealized_pnl, json.dumps(meta or {})],
    )


def get_latest_positions(conn: Optional[duckdb.DuckDBPyConnection] = None) -> List[dict]:
    c = conn or get_db()
    rows = c.execute(
        """
        SELECT * FROM positions
        WHERE (ticker, as_of) IN (
            SELECT ticker, MAX(as_of) FROM positions GROUP BY ticker
        )
        ORDER BY ticker
        """
    ).fetchall()
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# signals
# ═══════════════════════════════════════════════════════════════════════════════

def insert_signal(
    *,
    strategy: str,
    ticker: str,
    signal: str,
    as_of: str,
    meta: Optional[dict] = None,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> None:
    c = conn or get_db()
    c.execute(
        """
        INSERT INTO signals (strategy, ticker, signal, as_of, meta_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        [strategy, ticker, signal, as_of, json.dumps(meta or {})],
    )


def get_recent_signals(limit: int = 50, conn: Optional[duckdb.DuckDBPyConnection] = None) -> List[dict]:
    c = conn or get_db()
    rows = c.execute("SELECT * FROM signals ORDER BY created_at DESC LIMIT ?", [limit]).fetchall()
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# ingestion_runs
# ═══════════════════════════════════════════════════════════════════════════════

def insert_ingestion_run(
    *,
    run_id: str,
    tickers: str,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> None:
    c = conn or get_db()
    c.execute(
        "INSERT INTO ingestion_runs (run_id, tickers) VALUES (?, ?)",
        [run_id, tickers],
    )


def complete_ingestion_run(
    *,
    run_id: str,
    status: str = "success",
    rows_total: int = 0,
    error_summary: str = "",
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> None:
    c = conn or get_db()
    c.execute(
        """
        UPDATE ingestion_runs
        SET ended_at = current_timestamp, status = ?, rows_total = ?, error_summary = ?
        WHERE run_id = ?
        """,
        [status, rows_total, error_summary, run_id],
    )


def list_ingestion_runs(limit: int = 20, conn: Optional[duckdb.DuckDBPyConnection] = None) -> List[dict]:
    c = conn or get_db()
    rows = c.execute(
        "SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT ?", [limit]
    ).fetchall()
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# order_requests (audit log)
# ═══════════════════════════════════════════════════════════════════════════════

def log_order_request(
    *,
    client_ip: str = "",
    ticker: str,
    side: str,
    qty: float,
    requested_price: float = 0,
    accepted: bool = False,
    reject_reason: str = "",
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> None:
    c = conn or get_db()
    c.execute(
        """
        INSERT INTO order_requests (client_ip, ticker, side, qty, requested_price, accepted, reject_reason)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [client_ip, ticker, side, qty, requested_price, accepted, reject_reason],
    )


def get_todays_order_count(conn: Optional[duckdb.DuckDBPyConnection] = None) -> int:
    c = conn or get_db()
    rows = c.execute(
        "SELECT COUNT(*) FROM order_requests WHERE created_at >= current_date"
    ).fetchone()
    return rows[0] if rows else 0


def get_todays_paper_pnl(conn: Optional[duckdb.DuckDBPyConnection] = None) -> float:
    c = conn or get_db()
    rows = c.execute(
        "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE run_id = 'paper' AND entry_time >= CAST(current_date AS VARCHAR)"
    ).fetchone()
    return float(rows[0]) if rows else 0.0
