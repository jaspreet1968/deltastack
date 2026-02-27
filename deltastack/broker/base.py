"""Broker abstraction layer.

All broker implementations (paper, live) must subclass ``Broker`` and
implement the three core methods.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class OrderRequest:
    ticker: str
    side: str          # BUY / SELL
    qty: float
    order_type: str = "MARKET"   # MARKET / LIMIT
    limit_price: Optional[float] = None


@dataclass
class OrderResult:
    order_id: str
    ticker: str
    side: str
    qty: float
    fill_price: float
    commission: float
    status: str         # FILLED / REJECTED / PENDING
    message: str = ""


@dataclass
class Position:
    ticker: str
    qty: float
    avg_price: float
    market_price: float
    unrealized_pnl: float


@dataclass
class Account:
    cash: float
    equity: float
    positions_value: float
    num_positions: int


class Broker(ABC):
    """Interface that all broker adapters must implement."""

    @abstractmethod
    def place_order(self, order: OrderRequest) -> OrderResult:
        ...

    @abstractmethod
    def get_positions(self) -> List[Position]:
        ...

    @abstractmethod
    def get_account(self) -> Account:
        ...

    @abstractmethod
    def list_orders(self, limit: int = 20) -> List[dict]:
        """Return recent orders as dicts."""
        ...
