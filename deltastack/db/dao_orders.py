"""DAO for orders table and errors table (Phase G)."""

from __future__ import annotations

import json
import logging
from typing import List, Optional

from deltastack.db.connection import get_db

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# orders
# ═══════════════════════════════════════════════════════════════════════════════

def insert_order(
    *,
    order_id: str,
    provider: str = "paper",
    status: str = "CREATED",
    request_json: str = "{}",
    response_json: str = "{}",
    filled_qty: float = 0,
    avg_fill_price: float = 0,
    idempotency_key: str = "",
) -> None:
    c = get_db()
    c.execute(
        """INSERT INTO orders (order_id, provider, status, request_json, response_json,
           filled_qty, avg_fill_price, idempotency_key)
           VALUES (?,?,?,?,?,?,?,?)""",
        [order_id, provider, status, request_json, response_json,
         filled_qty, avg_fill_price, idempotency_key],
    )


def update_order_status(order_id: str, status: str, response_json: str = "",
                         filled_qty: float = 0, avg_fill_price: float = 0) -> None:
    c = get_db()
    c.execute(
        """UPDATE orders SET status=?, updated_at=current_timestamp, response_json=?,
           filled_qty=?, avg_fill_price=?
           WHERE order_id=?""",
        [status, response_json, filled_qty, avg_fill_price, order_id],
    )


def get_order(order_id: str) -> Optional[dict]:
    c = get_db()
    rows = c.execute("SELECT * FROM orders WHERE order_id = ?", [order_id]).fetchall()
    if not rows:
        return None
    cols = [d[0] for d in c.description]
    return dict(zip(cols, rows[0]))


def get_order_by_idempotency_key(key: str) -> Optional[dict]:
    if not key:
        return None
    c = get_db()
    rows = c.execute("SELECT * FROM orders WHERE idempotency_key = ? LIMIT 1", [key]).fetchall()
    if not rows:
        return None
    cols = [d[0] for d in c.description]
    return dict(zip(cols, rows[0]))


def list_orders(limit: int = 50) -> List[dict]:
    c = get_db()
    rows = c.execute("SELECT * FROM orders ORDER BY created_at DESC LIMIT ?", [limit]).fetchall()
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in rows]


def count_orders_today() -> int:
    c = get_db()
    row = c.execute("SELECT COUNT(*) FROM orders WHERE created_at >= current_date").fetchone()
    return row[0] if row else 0


# ═══════════════════════════════════════════════════════════════════════════════
# errors
# ═══════════════════════════════════════════════════════════════════════════════

def log_error(
    *,
    component: str,
    severity: str = "error",
    message: str = "",
    context: Optional[dict] = None,
) -> None:
    c = get_db()
    c.execute(
        "INSERT INTO errors (component, severity, message, context_json) VALUES (?,?,?,?)",
        [component, severity, message, json.dumps(context or {})],
    )


def list_errors(limit: int = 50) -> List[dict]:
    c = get_db()
    rows = c.execute("SELECT * FROM errors ORDER BY created_at DESC LIMIT ?", [limit]).fetchall()
    cols = [d[0] for d in c.description]
    result = []
    for r in rows:
        row = dict(zip(cols, r))
        row["created_at"] = str(row["created_at"]) if row.get("created_at") else None
        result.append(row)
    return result
