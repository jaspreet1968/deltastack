"""Tradier sandbox broker adapter.

Uses the Tradier REST API directly via requests.
ONLY sandbox mode is supported – production URLs are blocked.
"""

from __future__ import annotations

import logging
from typing import List

import requests

from deltastack.broker.base import Account, Broker, OrderRequest, OrderResult, Position
from deltastack.config import get_settings

logger = logging.getLogger(__name__)

_SANDBOX_URLS = (
    "https://sandbox.tradier.com",
)


def _is_sandbox_url(url: str) -> bool:
    return any(url.rstrip("/").startswith(p) for p in _SANDBOX_URLS)


class TradierBroker(Broker):
    """Tradier REST adapter – SANDBOX mode only."""

    def __init__(self) -> None:
        settings = get_settings()
        self.base_url = settings.tradier_base_url.rstrip("/")
        self.token = settings.tradier_access_token
        self._last_error: str = ""

        if not _is_sandbox_url(self.base_url):
            raise RuntimeError(
                f"SAFETY: Tradier base URL '{self.base_url}' is NOT a sandbox endpoint. "
                "DeltaStack refuses to connect to production. Set TRADIER_BASE_URL to sandbox."
            )

        if not self.token:
            raise RuntimeError("TRADIER_ACCESS_TOKEN must be set in .env")

    @property
    def headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }

    def _get(self, path: str, params: dict | None = None) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = requests.get(url, headers=self.headers, params=params, timeout=15)
        if resp.status_code >= 400:
            self._last_error = f"{resp.status_code}: {resp.text[:200]}"
        return resp

    def _post(self, path: str, data: dict) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = requests.post(url, headers=self.headers, data=data, timeout=15)
        if resp.status_code >= 400:
            self._last_error = f"{resp.status_code}: {resp.text[:200]}"
        return resp

    # ── Broker interface ─────────────────────────────────────────────────

    def place_order(self, order: OrderRequest) -> OrderResult:
        # Tradier uses account_id; for sandbox, use default
        resp = self._get("/user/profile")
        if resp.status_code != 200:
            return OrderResult(
                order_id="", ticker=order.ticker, side=order.side,
                qty=order.qty, fill_price=0, commission=0,
                status="REJECTED", message="Cannot fetch account profile",
            )
        profile = resp.json()
        account_id = ""
        try:
            account_id = profile["profile"]["account"]["account_number"]
        except (KeyError, TypeError):
            try:
                accounts = profile["profile"]["account"]
                if isinstance(accounts, list):
                    account_id = accounts[0]["account_number"]
            except (KeyError, TypeError, IndexError):
                pass

        if not account_id:
            return OrderResult(
                order_id="", ticker=order.ticker, side=order.side,
                qty=order.qty, fill_price=0, commission=0,
                status="REJECTED", message="Cannot determine Tradier account_id",
            )

        data = {
            "class": "equity",
            "symbol": order.ticker.upper(),
            "side": order.side.lower(),
            "quantity": str(int(order.qty)),
            "type": "market" if order.order_type == "MARKET" else "limit",
            "duration": "day",
        }
        if order.order_type == "LIMIT" and order.limit_price:
            data["price"] = str(order.limit_price)

        resp = self._post(f"/accounts/{account_id}/orders", data)
        if resp.status_code in (200, 201):
            body = resp.json()
            oid = body.get("order", {}).get("id", "")
            return OrderResult(
                order_id=str(oid),
                ticker=order.ticker.upper(),
                side=order.side.upper(),
                qty=order.qty,
                fill_price=0,  # Tradier fills asynchronously
                commission=0,
                status="PENDING",
                message=f"Tradier order {oid}",
            )
        return OrderResult(
            order_id="", ticker=order.ticker, side=order.side,
            qty=order.qty, fill_price=0, commission=0,
            status="REJECTED", message=self._last_error,
        )

    def get_positions(self) -> List[Position]:
        resp = self._get("/user/profile")
        if resp.status_code != 200:
            return []
        try:
            account_id = resp.json()["profile"]["account"]["account_number"]
        except (KeyError, TypeError):
            return []

        resp = self._get(f"/accounts/{account_id}/positions")
        if resp.status_code != 200:
            return []
        body = resp.json()
        positions_data = body.get("positions", {}).get("position", [])
        if isinstance(positions_data, dict):
            positions_data = [positions_data]
        return [
            Position(
                ticker=p.get("symbol", ""),
                qty=float(p.get("quantity", 0)),
                avg_price=float(p.get("cost_basis", 0)) / max(float(p.get("quantity", 1)), 1),
                market_price=float(p.get("last_price", 0)),
                unrealized_pnl=float(p.get("unrealized_pnl", 0)),
            )
            for p in positions_data
        ]

    def get_account(self) -> Account:
        resp = self._get("/user/profile")
        if resp.status_code != 200:
            return Account(cash=0, equity=0, positions_value=0, num_positions=0)
        try:
            account_id = resp.json()["profile"]["account"]["account_number"]
        except (KeyError, TypeError):
            return Account(cash=0, equity=0, positions_value=0, num_positions=0)

        resp = self._get(f"/accounts/{account_id}/balances")
        if resp.status_code != 200:
            return Account(cash=0, equity=0, positions_value=0, num_positions=0)
        b = resp.json().get("balances", {})
        return Account(
            cash=float(b.get("total_cash", 0)),
            equity=float(b.get("total_equity", 0)),
            positions_value=float(b.get("market_value", 0)),
            num_positions=0,
        )

    def list_orders(self, limit: int = 20) -> list:
        resp = self._get("/user/profile")
        if resp.status_code != 200:
            return []
        try:
            account_id = resp.json()["profile"]["account"]["account_number"]
        except (KeyError, TypeError):
            return []

        resp = self._get(f"/accounts/{account_id}/orders")
        if resp.status_code != 200:
            return []
        orders = resp.json().get("orders", {}).get("order", [])
        if isinstance(orders, dict):
            orders = [orders]
        return [
            {
                "order_id": str(o.get("id", "")),
                "ticker": o.get("symbol", ""),
                "side": o.get("side", ""),
                "qty": o.get("quantity", ""),
                "status": o.get("status", ""),
                "created_at": o.get("create_date", ""),
            }
            for o in orders[:limit]
        ]

    @property
    def last_error(self) -> str:
        return self._last_error
