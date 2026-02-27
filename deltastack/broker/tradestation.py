"""TradeStation simulator (SIM) broker adapter.

Uses the TradeStation REST API v3 directly via requests.
ONLY SIM mode endpoints are supported – production URLs are blocked.
"""

from __future__ import annotations

import logging
from typing import List

import requests

from deltastack.broker.base import Account, Broker, OrderRequest, OrderResult, Position
from deltastack.config import get_settings

logger = logging.getLogger(__name__)

_SIM_URLS = (
    "https://sim-api.tradestation.com",
    "https://sim.api.tradestation.com",
)


def _is_sim_url(url: str) -> bool:
    return any(url.rstrip("/").startswith(p) for p in _SIM_URLS)


class TradeStationBroker(Broker):
    """TradeStation REST adapter – SIM (simulator) mode only."""

    def __init__(self) -> None:
        settings = get_settings()
        self.base_url = settings.tradestation_base_url.rstrip("/")
        self.client_id = settings.tradestation_client_id
        self.client_secret = settings.tradestation_client_secret
        self._access_token: str = ""
        self._last_error: str = ""

        if not _is_sim_url(self.base_url):
            raise RuntimeError(
                f"SAFETY: TradeStation base URL '{self.base_url}' is NOT a SIM endpoint. "
                "DeltaStack refuses to connect to production. Set TRADESTATION_BASE_URL to SIM."
            )

        if not self.client_id or not self.client_secret:
            raise RuntimeError(
                "TRADESTATION_CLIENT_ID and TRADESTATION_CLIENT_SECRET must be set in .env"
            )

    @property
    def headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self._access_token:
            h["Authorization"] = f"Bearer {self._access_token}"
        return h

    def _authenticate(self) -> bool:
        """Obtain OAuth2 access token using client credentials."""
        try:
            resp = requests.post(
                "https://signin.tradestation.com/oauth/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "audience": "https://api.tradestation.com",
                },
                timeout=15,
            )
            if resp.status_code == 200:
                self._access_token = resp.json().get("access_token", "")
                return bool(self._access_token)
            self._last_error = f"Auth failed: {resp.status_code}"
            return False
        except Exception as exc:
            self._last_error = f"Auth error: {exc}"
            return False

    def _get(self, path: str) -> requests.Response:
        if not self._access_token:
            self._authenticate()
        url = f"{self.base_url}{path}"
        resp = requests.get(url, headers=self.headers, timeout=15)
        if resp.status_code == 401:
            self._authenticate()
            resp = requests.get(url, headers=self.headers, timeout=15)
        if resp.status_code >= 400:
            self._last_error = f"{resp.status_code}: {resp.text[:200]}"
        return resp

    def _post(self, path: str, data: dict) -> requests.Response:
        if not self._access_token:
            self._authenticate()
        url = f"{self.base_url}{path}"
        resp = requests.post(url, headers=self.headers, json=data, timeout=15)
        if resp.status_code == 401:
            self._authenticate()
            resp = requests.post(url, headers=self.headers, json=data, timeout=15)
        if resp.status_code >= 400:
            self._last_error = f"{resp.status_code}: {resp.text[:200]}"
        return resp

    # ── Broker interface ─────────────────────────────────────────────────

    def place_order(self, order: OrderRequest) -> OrderResult:
        # Get account IDs first
        accounts = self._get_account_ids()
        if not accounts:
            return OrderResult(
                order_id="", ticker=order.ticker, side=order.side,
                qty=order.qty, fill_price=0, commission=0,
                status="REJECTED", message="Cannot fetch TradeStation accounts",
            )

        account_id = accounts[0]
        data = {
            "AccountID": account_id,
            "Symbol": order.ticker.upper(),
            "Quantity": str(int(order.qty)),
            "OrderType": "Market" if order.order_type == "MARKET" else "Limit",
            "TradeAction": "Buy" if order.side.upper() == "BUY" else "Sell",
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        if order.order_type == "LIMIT" and order.limit_price:
            data["LimitPrice"] = str(order.limit_price)

        resp = self._post(f"/orderexecution/orders", data)
        if resp.status_code in (200, 201):
            body = resp.json()
            orders = body.get("Orders", [{}])
            oid = orders[0].get("OrderID", "") if orders else ""
            return OrderResult(
                order_id=str(oid),
                ticker=order.ticker.upper(),
                side=order.side.upper(),
                qty=order.qty,
                fill_price=0,
                commission=0,
                status="PENDING",
                message=f"TradeStation order {oid}",
            )
        return OrderResult(
            order_id="", ticker=order.ticker, side=order.side,
            qty=order.qty, fill_price=0, commission=0,
            status="REJECTED", message=self._last_error,
        )

    def get_positions(self) -> List[Position]:
        accounts = self._get_account_ids()
        if not accounts:
            return []
        resp = self._get(f"/brokerage/accounts/{accounts[0]}/positions")
        if resp.status_code != 200:
            return []
        positions = []
        for p in resp.json().get("Positions", []):
            positions.append(Position(
                ticker=p.get("Symbol", ""),
                qty=float(p.get("Quantity", 0)),
                avg_price=float(p.get("AveragePrice", 0)),
                market_price=float(p.get("Last", 0)),
                unrealized_pnl=float(p.get("UnrealizedProfitLoss", 0)),
            ))
        return positions

    def get_account(self) -> Account:
        accounts = self._get_account_ids()
        if not accounts:
            return Account(cash=0, equity=0, positions_value=0, num_positions=0)
        resp = self._get(f"/brokerage/accounts/{accounts[0]}/balances")
        if resp.status_code != 200:
            return Account(cash=0, equity=0, positions_value=0, num_positions=0)
        b = resp.json().get("Balances", [{}])
        bal = b[0] if b else {}
        return Account(
            cash=float(bal.get("CashBalance", 0)),
            equity=float(bal.get("Equity", 0)),
            positions_value=float(bal.get("MarketValue", 0)),
            num_positions=0,
        )

    def list_orders(self, limit: int = 20) -> list:
        accounts = self._get_account_ids()
        if not accounts:
            return []
        resp = self._get(f"/brokerage/accounts/{accounts[0]}/orders")
        if resp.status_code != 200:
            return []
        orders = resp.json().get("Orders", [])
        return [
            {
                "order_id": str(o.get("OrderID", "")),
                "ticker": o.get("Legs", [{}])[0].get("Symbol", "") if o.get("Legs") else "",
                "side": o.get("Legs", [{}])[0].get("BuyOrSell", "") if o.get("Legs") else "",
                "qty": o.get("Legs", [{}])[0].get("QuantityOrdered", "") if o.get("Legs") else "",
                "status": o.get("Status", ""),
                "created_at": o.get("OpenedDateTime", ""),
            }
            for o in orders[:limit]
        ]

    # ── internal ─────────────────────────────────────────────────────────

    def _get_account_ids(self) -> List[str]:
        resp = self._get("/brokerage/accounts")
        if resp.status_code != 200:
            return []
        accounts = resp.json().get("Accounts", [])
        return [a.get("AccountID", "") for a in accounts if a.get("AccountID")]

    @property
    def last_error(self) -> str:
        return self._last_error
