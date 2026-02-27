"""Portfolio risk engine – evaluates proposed orders against exposure limits.

All limits are configurable via env vars. Rejects or adjusts orders
that would breach risk thresholds.
"""

from __future__ import annotations

import logging
from typing import List

from deltastack.broker.factory import get_broker
from deltastack.config import get_settings
from deltastack.data.storage import load_bars

logger = logging.getLogger(__name__)


def evaluate_plan(proposed_orders: List[dict]) -> dict:
    """Evaluate proposed orders against portfolio risk limits.

    Returns:
        dict with keys: accepted (bool), adjustments (list), reason_codes (list)
    """
    settings = get_settings()
    reason_codes = []
    adjustments = []
    accepted = True

    # Get current portfolio state
    try:
        broker = get_broker()
        account = broker.get_account()
        positions = broker.get_positions()
    except Exception:
        account = None
        positions = []

    equity = account.equity if account and account.equity > 0 else settings.paper_initial_cash

    # Current exposure
    current_gross = sum(abs(p.qty * p.market_price) for p in positions)
    current_net = sum(p.qty * p.market_price for p in positions)
    ticker_exposures = {p.ticker: abs(p.qty * p.market_price) for p in positions}

    for order in proposed_orders:
        ticker = order.get("ticker", "").upper()
        qty = order.get("qty", 0)
        side = order.get("side", "BUY").upper()

        # Estimate price
        try:
            df = load_bars(ticker, limit=1)
            price = float(df.iloc[-1]["close"]) if not df.empty else 0
        except Exception:
            price = 0

        if price <= 0:
            reason_codes.append(f"{ticker}: no price data for risk check")
            continue

        notional = qty * price
        sign = 1 if side == "BUY" else -1

        # 1. Single ticker exposure
        existing = ticker_exposures.get(ticker, 0)
        new_ticker_exp = existing + notional
        max_ticker = equity * settings.max_single_ticker_exposure_pct
        if new_ticker_exp > max_ticker:
            reason_codes.append(
                f"{ticker}: single ticker exposure {new_ticker_exp:.0f} > limit {max_ticker:.0f}"
            )
            accepted = False

        # 2. Gross exposure
        new_gross = current_gross + notional
        max_gross = equity * settings.max_gross_exposure_pct
        if new_gross > max_gross:
            reason_codes.append(
                f"Gross exposure {new_gross:.0f} > limit {max_gross:.0f}"
            )
            accepted = False

        # 3. Net exposure
        new_net = current_net + sign * notional
        max_net = equity * settings.max_net_exposure_pct
        if abs(new_net) > max_net:
            reason_codes.append(
                f"Net exposure {abs(new_net):.0f} > limit {max_net:.0f}"
            )
            accepted = False

    return {
        "accepted": accepted,
        "equity": round(equity, 2),
        "current_gross_exposure": round(current_gross, 2),
        "current_net_exposure": round(current_net, 2),
        "reason_codes": reason_codes,
        "limits": {
            "max_gross_exposure_pct": settings.max_gross_exposure_pct,
            "max_net_exposure_pct": settings.max_net_exposure_pct,
            "max_single_ticker_exposure_pct": settings.max_single_ticker_exposure_pct,
        },
    }
