"""Paper (simulated) broker – fills orders using last close price + slippage.

All state is persisted to DuckDB so positions survive restarts.
No real money is ever touched.
"""

from __future__ import annotations

import logging
import uuid
from typing import Dict, List

from deltastack.broker.base import Account, Broker, OrderRequest, OrderResult, Position
from deltastack.config import get_settings
from deltastack.data.storage import load_bars
from deltastack.db.dao import insert_trade, upsert_position, get_latest_positions

logger = logging.getLogger(__name__)


class PaperBroker(Broker):
    """Simulated broker backed by stored price data + DuckDB positions."""

    def __init__(self) -> None:
        settings = get_settings()
        self.commission = settings.default_commission
        self.slippage_bps = settings.default_slippage_bps
        self._cash: float | None = None  # lazy-loaded from settings

    @property
    def cash(self) -> float:
        if self._cash is None:
            self._cash = get_settings().paper_initial_cash
        return self._cash

    @cash.setter
    def cash(self, value: float) -> None:
        self._cash = value

    # ── core interface ───────────────────────────────────────────────────

    def place_order(self, order: OrderRequest) -> OrderResult:
        order_id = uuid.uuid4().hex[:12]
        ticker = order.ticker.upper()
        logger.info("Paper order %s: %s %s qty=%.4f", order_id, order.side, ticker, order.qty)

        # Get latest close price for fill simulation
        try:
            df = load_bars(ticker, limit=1)
            if df.empty:
                return OrderResult(
                    order_id=order_id, ticker=ticker, side=order.side,
                    qty=order.qty, fill_price=0, commission=0,
                    status="REJECTED", message=f"No price data for {ticker}",
                )
            last_price = float(df.iloc[-1]["close"])
        except FileNotFoundError:
            return OrderResult(
                order_id=order_id, ticker=ticker, side=order.side,
                qty=order.qty, fill_price=0, commission=0,
                status="REJECTED", message=f"No price data for {ticker}",
            )

        # Apply slippage
        slip = self.slippage_bps / 10_000.0
        if order.side.upper() == "BUY":
            fill_price = last_price * (1 + slip)
        else:
            fill_price = last_price * (1 - slip)

        cost = order.qty * fill_price
        total_cost = cost + self.commission

        # Check affordability for buys
        if order.side.upper() == "BUY" and total_cost > self.cash:
            return OrderResult(
                order_id=order_id, ticker=ticker, side=order.side,
                qty=order.qty, fill_price=fill_price, commission=self.commission,
                status="REJECTED", message=f"Insufficient cash: need {total_cost:.2f}, have {self.cash:.2f}",
            )

        # Execute
        if order.side.upper() == "BUY":
            self.cash -= total_cost
            self._update_position(ticker, order.qty, fill_price, "BUY")
        else:
            self.cash += cost - self.commission
            self._update_position(ticker, -order.qty, fill_price, "SELL")

        # Persist trade
        insert_trade(
            run_id="paper",
            ticker=ticker,
            side=order.side.upper(),
            qty=order.qty,
            entry_time="",
            entry_price=fill_price,
            exit_time="",
            exit_price=0,
            pnl=0,
            meta={"order_id": order_id, "commission": self.commission, "slippage_bps": self.slippage_bps},
        )

        logger.info(
            "Paper fill %s: %s %s %.4f @ %.4f commission=%.2f cash=%.2f",
            order_id, order.side, ticker, order.qty, fill_price, self.commission, self.cash,
        )

        return OrderResult(
            order_id=order_id,
            ticker=ticker,
            side=order.side.upper(),
            qty=order.qty,
            fill_price=round(fill_price, 4),
            commission=self.commission,
            status="FILLED",
            message="Paper fill",
        )

    def get_positions(self) -> List[Position]:
        rows = get_latest_positions()
        positions = []
        for r in rows:
            qty = r.get("qty", 0)
            if abs(qty) < 1e-9:
                continue
            avg = r.get("avg_price", 0)
            # Try to get current market price
            ticker = r.get("ticker", "")
            try:
                df = load_bars(ticker, limit=1)
                mkt = float(df.iloc[-1]["close"]) if not df.empty else avg
            except Exception:
                mkt = avg
            pnl = (mkt - avg) * qty
            positions.append(Position(
                ticker=ticker, qty=qty, avg_price=round(avg, 4),
                market_price=round(mkt, 4), unrealized_pnl=round(pnl, 2),
            ))
        return positions

    def get_account(self) -> Account:
        positions = self.get_positions()
        pos_value = sum(p.qty * p.market_price for p in positions)
        return Account(
            cash=round(self.cash, 2),
            equity=round(self.cash + pos_value, 2),
            positions_value=round(pos_value, 2),
            num_positions=len(positions),
        )

    def list_orders(self, limit: int = 20) -> list:
        """Return recent paper trades from DB."""
        from deltastack.db.dao import get_trades_for_run
        trades = get_trades_for_run("paper")
        return trades[-limit:] if len(trades) > limit else trades

    # ── internal ─────────────────────────────────────────────────────────

    def _update_position(self, ticker: str, qty_delta: float, price: float, side: str) -> None:
        """Update position in DB (append-only ledger)."""
        # Get current position
        rows = get_latest_positions()
        current = next((r for r in rows if r.get("ticker") == ticker), None)

        if current:
            old_qty = current.get("qty", 0)
            old_avg = current.get("avg_price", 0)
            new_qty = old_qty + qty_delta
            if abs(new_qty) < 1e-9:
                new_avg = 0
            elif qty_delta > 0:
                # Weighted average for buys
                new_avg = (old_qty * old_avg + qty_delta * price) / new_qty if new_qty > 0 else 0
            else:
                new_avg = old_avg  # sells don't change avg price
        else:
            new_qty = qty_delta
            new_avg = price if qty_delta > 0 else 0

        upsert_position(
            ticker=ticker,
            qty=round(new_qty, 6),
            avg_price=round(new_avg, 4),
            unrealized_pnl=0,
            meta={"last_side": side, "last_price": round(price, 4)},
        )


# Singleton for the app lifetime
_paper_broker: PaperBroker | None = None


def get_paper_broker() -> PaperBroker:
    global _paper_broker
    if _paper_broker is None:
        _paper_broker = PaperBroker()
    return _paper_broker
