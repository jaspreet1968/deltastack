"""Execution workflow: plan -> confirm -> execute (paper only).

Plans are recorded in DuckDB and must be explicitly confirmed before
orders are placed.  This prevents accidental one-click trading.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from deltastack.broker.base import OrderRequest
from deltastack.broker.factory import get_broker
from deltastack.config import get_settings
from deltastack.db.dao import log_order_request, get_todays_order_count, get_latest_positions
from deltastack.db.dao_orders import insert_order, update_order_status, get_order_by_idempotency_key
from deltastack.db.dao_options import (
    insert_execution_plan,
    get_execution_plan,
    update_plan_status,
    insert_execution_event,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/execute", tags=["execute"])


def _check_kill_switch() -> None:
    settings = get_settings()
    if not settings.trading_enabled:
        raise HTTPException(
            status_code=503,
            detail="Trading is disabled. Set TRADING_ENABLED=true in .env and restart.",
        )


class PlanRequest(BaseModel):
    strategy: str = Field(..., examples=["sma"])
    ticker: str = Field(..., examples=["AAPL"])
    side: str = Field("BUY", examples=["BUY"])
    qty: float = Field(10, gt=0)


class ConfirmRequest(BaseModel):
    plan_id: str = Field(..., examples=["abc123"])
    idempotency_key: str = Field("", description="Optional idempotency key to prevent duplicate orders")


# ── POST /execute/plan ───────────────────────────────────────────────────────

@router.post("/plan")
def create_plan(body: PlanRequest):
    """Create an execution plan (does NOT trade yet). Returns plan_id + proposed orders."""
    # Auth required but kill switch NOT checked here – planning is safe
    settings = get_settings()
    plan_id = uuid.uuid4().hex[:16]

    # Build proposed order
    order = {
        "ticker": body.ticker.upper(),
        "side": body.side.upper(),
        "qty": body.qty,
        "order_type": "MARKET",
    }

    # Risk summary
    from deltastack.data.storage import load_bars
    try:
        df = load_bars(body.ticker, limit=1)
        est_price = float(df.iloc[-1]["close"]) if not df.empty else 0
    except Exception:
        est_price = 0

    notional = body.qty * est_price
    risk_summary = {
        "estimated_price": round(est_price, 4),
        "estimated_notional": round(notional, 2),
        "max_notional_limit": settings.max_notional_per_order,
        "within_limit": notional <= settings.max_notional_per_order,
        "daily_orders_used": get_todays_order_count(),
        "daily_order_limit": settings.max_daily_orders,
        "require_confirm": settings.execution_require_confirm,
    }

    # Persist plan
    insert_execution_plan(
        plan_id=plan_id,
        request_json=json.dumps(body.model_dump()),
        orders_json=json.dumps([order]),
        risk_summary=json.dumps(risk_summary),
        status="pending",
    )
    insert_execution_event(plan_id=plan_id, event_type="plan_created", details=risk_summary)

    logger.info("Execution plan created: plan_id=%s %s %s qty=%.2f", plan_id, body.side, body.ticker, body.qty)

    return {
        "plan_id": plan_id,
        "status": "pending",
        "orders": [order],
        "risk_summary": risk_summary,
        "next_step": "POST /execute/confirm with plan_id to execute" if settings.execution_require_confirm else "auto-execute disabled",
    }


# ── POST /execute/confirm ────────────────────────────────────────────────────

@router.post("/confirm")
def confirm_plan(body: ConfirmRequest, request: Request):
    """Confirm and execute a pending plan. Requires TRADING_ENABLED=true."""
    _check_kill_switch()

    # Idempotency check
    if body.idempotency_key:
        existing = get_order_by_idempotency_key(body.idempotency_key)
        if existing:
            logger.info("Idempotency key %s already used – returning prior result", body.idempotency_key)
            return {
                "plan_id": body.plan_id,
                "status": "already_executed",
                "idempotency_key": body.idempotency_key,
                "prior_order_id": existing.get("order_id", ""),
            }

    plan = get_execution_plan(body.plan_id)
    if plan is None:
        raise HTTPException(status_code=404, detail=f"Plan {body.plan_id} not found")

    if plan["status"] != "pending":
        raise HTTPException(status_code=400, detail=f"Plan status is '{plan['status']}', not 'pending'")

    orders = json.loads(plan["orders_json"])
    risk = json.loads(plan["risk_summary"])

    # Verify risk is still acceptable
    if not risk.get("within_limit", True):
        update_plan_status(body.plan_id, "rejected")
        insert_execution_event(plan_id=body.plan_id, event_type="risk_rejected", details=risk)
        raise HTTPException(status_code=400, detail="Risk check failed: notional exceeds limit")

    # Execute orders via paper broker
    broker = get_broker()
    results = []
    ip = request.headers.get("X-Real-IP", request.client.host if request.client else "unknown")

    for order in orders:
        req = OrderRequest(
            ticker=order["ticker"],
            side=order["side"],
            qty=order["qty"],
            order_type=order.get("order_type", "MARKET"),
        )
        result = broker.place_order(req)
        results.append(asdict(result))

        # Write to orders lifecycle table
        insert_order(
            order_id=result.order_id or uuid.uuid4().hex[:12],
            provider=settings.broker_provider,
            status=result.status,
            request_json=json.dumps(order),
            response_json=json.dumps(asdict(result)),
            filled_qty=result.qty if result.status == "FILLED" else 0,
            avg_fill_price=result.fill_price,
            idempotency_key=body.idempotency_key,
        )

        log_order_request(
            client_ip=ip,
            ticker=order["ticker"],
            side=order["side"],
            qty=order["qty"],
            requested_price=result.fill_price,
            accepted=result.status == "FILLED",
            reject_reason="" if result.status == "FILLED" else result.message,
        )

    update_plan_status(body.plan_id, "executed")
    insert_execution_event(
        plan_id=body.plan_id,
        event_type="executed",
        details={"results": results},
    )

    logger.info("Plan %s executed: %d orders", body.plan_id, len(results))

    return {
        "plan_id": body.plan_id,
        "status": "executed",
        "results": results,
    }
