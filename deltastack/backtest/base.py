"""Abstract base class for backtesting strategies.

Every strategy must subclass ``Strategy`` and implement:
* ``run(df)`` → ``BacktestResult``
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import date
from typing import Any, Dict, List

import numpy as np
import pandas as pd


@dataclass
class BacktestResult:
    """Standardised output for any strategy backtest."""

    strategy: str
    ticker: str
    start: str
    end: str
    params: Dict[str, Any]
    total_return: float
    cagr: float
    max_drawdown: float
    num_trades: int
    win_rate: float
    sharpe_like: float
    trades: List[dict]

    def to_dict(self) -> dict:
        return asdict(self)


class Strategy(ABC):
    """Interface that all backtest strategies must implement."""

    name: str = "base"

    @abstractmethod
    def run(self, df: pd.DataFrame, **params) -> BacktestResult:
        """Execute the strategy on a price DataFrame and return metrics."""
        ...

    # ── shared helpers ───────────────────────────────────────────────────

    @staticmethod
    def compute_cagr(start_equity: float, end_equity: float, days: int) -> float:
        years = max(days / 365.25, 0.01)
        if end_equity <= 0:
            return -1.0
        return (end_equity / start_equity) ** (1.0 / years) - 1.0

    @staticmethod
    def compute_max_drawdown(equity_curve: List[float]) -> float:
        eq = np.array(equity_curve) if equity_curve else np.array([1.0])
        peak = np.maximum.accumulate(eq)
        dd = (eq - peak) / np.where(peak > 0, peak, 1.0)
        return float(dd.min())

    @staticmethod
    def compute_sharpe(cagr: float, equity_curve: List[float]) -> float:
        if len(equity_curve) <= 1:
            return 0.0
        daily_ret = pd.Series(equity_curve).pct_change().dropna()
        ann_vol = float(daily_ret.std() * math.sqrt(252)) if len(daily_ret) > 1 else 1.0
        return cagr / ann_vol if ann_vol > 0 else 0.0
