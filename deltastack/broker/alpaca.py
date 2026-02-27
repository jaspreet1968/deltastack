"""Alpaca paper trading broker adapter.

Uses the Alpaca REST API directly (no external SDK dependency).
ONLY paper mode is supported – live URLs are blocked.
"""

from __future__ import annotations

import logging
from typing import List

import requests

from deltastack.broker.base import Account, Broker, OrderRequest, OrderResult, Position
from deltastack.config import get_settings

logger = logging.getLogger(__name__)

_PAPER_URLS = (
    "https://paper-api.alpaca.markets",
    "https://broker-api.sandbox.alpaca.markets",
)


def _is_paper_url(url: str) -> bool:
    return any(url.rstrip("/").startswith(p) for p in _PAPER_URLS)


class AlpacaBroker(Broker):
    """Alpaca REST adapter – PAPER mode only."""

    def __init__(self) -> None:
        settings = get_settings()
        self.base_url = settings.alpaca_base_url.rstrip("/")
        self.api_key = settings.alpaca_api_key
        self.secret_key = settings.alpaca_secret_key
        self._last_error: str = ""

        # Hard block live
        if not _is_paper_url(self.base_url):
            raise RuntimeError(
                f"SAFETY: Alpaca base URL '{self.base_url}' is NOT a paper endpoint. "
                "DeltaStack refuses to connect to live. Set ALPACA_BASE_URL to a paper URL."
            )

        if not self.api_key or not self.secret_key:
            raise RuntimeError(
                "ALPACA_API_KEY and ALPACA_SECRET_KEY must be set in .env"
            )

    @property
    def headers(self) -> dict:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
            "Content-Type": "application/json",
        }

    def _get(self, path: str) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = requests.get(url, headers=self.headers, timeout=15)
        if resp.status_code >= 400:
            self._last_error = f"{resp.status_code}: {resp.text[:200]}"
            logger.warning("Alpaca GET %s -> %d", path, resp.status_code)
        return resp

    def _post(self, path: str, data: dict) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = requests.post(url, headers=self.headers, json=data, timeout=15)
        if resp.status_code >= 400:
            self._last_error = f"{resp.status_code}: {resp.text[:200]}"
            logger.warning("Alpaca POST %s -> %d", path, resp.status_code)
        return resp

    # ── Broker interface ─────────────────────────────────────────────────

    def place_order(self, order: OrderRequest) -> OrderResult:
        data = {
            "symbol": order.ticker.upper(),
            "qty": str(order.qty),
            "side": order.side.lower(),
            "type": "market" if order.order_type == "MARKET" else "limit",
            "time_in_force": "day",
        }
        if order.order_type == "LIMIT" and order.limit_price:
            data["limit_price"] = str(order.limit_price)

        resp = self._post("/v2/orders", data)
        if resp.status_code in (200, 201):
            body = resp.json()
            return OrderResult(
                order_id=body.get("id", ""),
                ticker=order.ticker.upper(),
                side=order.side.upper(),
                qty=order.qty,
                fill_price=float(body.get("filled_avg_price") or 0),
                commission=0,
                status="FILLED" if body.get("status") == "filled" else body.get("status", "PENDING").upper(),
                message=f"Alpaca order {body.get('id', '')}",
            )
        else:
            return OrderResult(
                order_id="",
                ticker=order.ticker.upper(),
                side=order.side.upper(),
                qty=order.qty,
                fill_price=0,
                commission=0,
                status="REJECTED",
                message=self._last_error,
            )

    def get_positions(self) -> List[Position]:
        resp = self._get("/v2/positions")
        if resp.status_code != 200:
            return []
        positions = []
        for p in resp.json():
            positions.append(Position(
                ticker=p.get("symbol", ""),
                qty=float(p.get("qty", 0)),
                avg_price=float(p.get("avg_entry_price", 0)),
                market_price=float(p.get("current_price", 0)),
                unrealized_pnl=float(p.get("unrealized_pl", 0)),
            ))
        return positions

    def get_account(self) -> Account:
        resp = self._get("/v2/account")
        if resp.status_code != 200:
            return Account(cash=0, equity=0, positions_value=0, num_positions=0)
        a = resp.json()
        return Account(
            cash=float(a.get("cash", 0)),
            equity=float(a.get("equity", 0)),
            positions_value=float(a.get("long_market_value", 0)) + float(a.get("short_market_value", 0)),
            num_positions=int(float(a.get("position_market_value", 0)) != 0),
        )

    def list_orders(self, limit: int = 20) -> list:
        resp = self._get(f"/v2/orders?limit={limit}&status=all")
        if resp.status_code != 200:
            return []
        return [
            {
                "order_id": o.get("id", ""),
                "ticker": o.get("symbol", ""),
                "side": o.get("side", ""),
                "qty": o.get("qty", ""),
                "status": o.get("status", ""),
                "filled_avg_price": o.get("filled_avg_price"),
                "created_at": o.get("created_at", ""),
            }
            for o in resp.json()
        ]

    @property
    def last_error(self) -> str:
        return self._last_error
