"""Trading endpoints – paper trading only, with risk controls.

All /trade/* endpoints are gated by the TRADING_ENABLED kill switch.
If TRADING_ENABLED is not "true", every request returns 503.
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from deltastack.broker.base import OrderRequest
from deltastack.broker.factory import get_broker
from deltastack.config import get_settings
from deltastack.db.dao import log_order_request, get_todays_order_count, get_todays_paper_pnl, get_latest_positions

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/trade", tags=["trade"])


def _check_kill_switch() -> None:
    settings = get_settings()
    if not settings.trading_enabled:
        raise HTTPException(
            status_code=503,
            detail="Trading is disabled. Set TRADING_ENABLED=true in .env and restart.",
        )


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Real-IP") or request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


class PlaceOrderRequest(BaseModel):
    ticker: str = Field(..., examples=["AAPL"])
    side: str = Field(..., examples=["BUY"])
    qty: float = Field(..., gt=0, examples=[10.0])
    order_type: str = Field("MARKET", examples=["MARKET"])
    limit_price: Optional[float] = None


# ── risk check ───────────────────────────────────────────────────────────────

def _check_risk(body: PlaceOrderRequest, client_ip: str) -> Optional[str]:
    """Return a reject reason string, or None if order is acceptable."""
    settings = get_settings()

    # 1. Max notional per order (estimate)
    from deltastack.data.storage import load_bars
    try:
        df = load_bars(body.ticker, limit=1)
        est_price = float(df.iloc[-1]["close"]) if not df.empty else 0
    except Exception:
        est_price = 0

    if est_price > 0:
        notional = body.qty * est_price
        if notional > settings.max_notional_per_order:
            return f"Notional {notional:.2f} exceeds limit {settings.max_notional_per_order:.2f}"

    # 2. Max daily orders
    daily_count = get_todays_order_count()
    if daily_count >= settings.max_daily_orders:
        return f"Daily order limit reached ({settings.max_daily_orders})"

    # 3. Max open positions (for buys only)
    if body.side.upper() == "BUY":
        positions = get_latest_positions()
        open_count = sum(1 for p in positions if abs(p.get("qty", 0)) > 1e-9)
        if open_count >= settings.max_open_positions:
            return f"Max open positions reached ({settings.max_open_positions})"

    # 4. Max daily loss
    daily_pnl = get_todays_paper_pnl()
    if daily_pnl < -settings.max_daily_loss:
        return f"Daily loss limit breached (P&L: {daily_pnl:.2f}, limit: -{settings.max_daily_loss:.2f})"

    return None


# ── POST /trade/order ────────────────────────────────────────────────────────

@router.post("/order")
def place_order(body: PlaceOrderRequest, request: Request):
    """Place a paper trade order with risk checks."""
    _check_kill_switch()
    ip = _client_ip(request)
    logger.info("Trade order from %s: %s %s qty=%.4f", ip, body.side, body.ticker, body.qty)

    # Risk check
    reject_reason = _check_risk(body, ip)
    if reject_reason:
        log_order_request(
            client_ip=ip, ticker=body.ticker, side=body.side, qty=body.qty,
            accepted=False, reject_reason=reject_reason,
        )
        raise HTTPException(status_code=400, detail=f"Risk check failed: {reject_reason}")

    broker = get_broker()
    req = OrderRequest(
        ticker=body.ticker,
        side=body.side.upper(),
        qty=body.qty,
        order_type=body.order_type,
        limit_price=body.limit_price,
    )
    result = broker.place_order(req)

    # Audit log
    log_order_request(
        client_ip=ip, ticker=body.ticker, side=body.side, qty=body.qty,
        requested_price=result.fill_price,
        accepted=result.status == "FILLED",
        reject_reason="" if result.status == "FILLED" else result.message,
    )

    return asdict(result)


# ── GET /trade/positions ─────────────────────────────────────────────────────

@router.get("/positions")
def get_positions():
    """Get current paper trading positions."""
    _check_kill_switch()
    broker = get_broker()
    positions = broker.get_positions()
    return {"positions": [asdict(p) for p in positions]}


# ── GET /trade/account ───────────────────────────────────────────────────────

@router.get("/account")
def get_account():
    """Get paper trading account summary."""
    _check_kill_switch()
    broker = get_broker()
    account = broker.get_account()
    return asdict(account)


# ── GET /trade/risk ──────────────────────────────────────────────────────────

@router.get("/risk")
def get_risk_status():
    """Return current risk limits and today's usage."""
    _check_kill_switch()
    settings = get_settings()
    daily_orders = get_todays_order_count()
    daily_pnl = get_todays_paper_pnl()
    positions = get_latest_positions()
    open_count = sum(1 for p in positions if abs(p.get("qty", 0)) > 1e-9)

    return {
        "limits": {
            "max_notional_per_order": settings.max_notional_per_order,
            "max_daily_orders": settings.max_daily_orders,
            "max_open_positions": settings.max_open_positions,
            "max_daily_loss": settings.max_daily_loss,
        },
        "usage_today": {
            "orders_placed": daily_orders,
            "orders_remaining": max(0, settings.max_daily_orders - daily_orders),
            "open_positions": open_count,
            "daily_pnl": round(daily_pnl, 2),
            "daily_loss_remaining": round(settings.max_daily_loss + daily_pnl, 2),
        },
    }
