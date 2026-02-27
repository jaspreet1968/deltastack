"""Simple Moving Average (SMA) crossover backtest engine.

Assumptions (v0.1)
------------------
* Long-only: buy when fast SMA crosses *above* slow SMA, sell on reverse.
* No slippage, no commissions (configurable later).
* Trades execute at the **close** of the signal day.
* Fully invested on each buy; fully flat on each sell.
* Cash earns 0 %.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, asdict
from datetime import date
from typing import List, Optional

import numpy as np
import pandas as pd

from deltastack.data.storage import load_bars

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    ticker: str
    start: str
    end: str
    fast_period: int
    slow_period: int
    total_return: float       # e.g. 0.42 = +42 %
    cagr: float               # annualised
    max_drawdown: float       # worst peak-to-trough (negative)
    num_trades: int
    win_rate: float           # fraction of winning trades
    sharpe_like: float        # annualised return / annualised vol (simplified)
    trades: List[dict]        # individual trade log

    def to_dict(self) -> dict:
        return asdict(self)


def run_sma_backtest(
    ticker: str,
    start: date,
    end: date,
    fast: int = 10,
    slow: int = 30,
) -> BacktestResult:
    """Execute an SMA crossover backtest on stored daily bars."""
    if fast >= slow:
        raise ValueError(f"fast ({fast}) must be < slow ({slow})")

    df = load_bars(ticker, start=start, end=end, limit=100_000)
    if df.empty:
        raise ValueError(f"No bars loaded for {ticker} in [{start}, {end}]")

    df = df.sort_values("date").reset_index(drop=True)
    df["close_f"] = df["close"].astype(float)
    df["sma_fast"] = df["close_f"].rolling(window=fast).mean()
    df["sma_slow"] = df["close_f"].rolling(window=slow).mean()

    # Drop rows where SMAs are not yet available
    df = df.dropna(subset=["sma_fast", "sma_slow"]).reset_index(drop=True)
    if len(df) < 2:
        raise ValueError("Not enough data after computing SMAs")

    # ── generate signals ─────────────────────────────────────────────────
    df["signal"] = 0
    df.loc[df["sma_fast"] > df["sma_slow"], "signal"] = 1   # bullish
    df.loc[df["sma_fast"] <= df["sma_slow"], "signal"] = -1  # bearish

    # Detect crossover points (signal changes)
    df["cross"] = df["signal"].diff().fillna(0).astype(int)

    # ── simulate trades ──────────────────────────────────────────────────
    trades: List[dict] = []
    position_open = False
    entry_price = 0.0
    entry_date: Optional[date] = None
    cash = 1.0  # normalised starting capital
    shares = 0.0
    equity_curve: List[float] = []

    for _, row in df.iterrows():
        price = float(row["close_f"])
        d = row["date"]

        if row["cross"] > 0 and not position_open:
            # BUY
            shares = cash / price
            entry_price = price
            entry_date = d
            cash = 0.0
            position_open = True

        elif row["cross"] < 0 and position_open:
            # SELL
            cash = shares * price
            pnl = (price - entry_price) / entry_price
            trades.append({
                "entry_date": str(entry_date),
                "exit_date": str(d),
                "entry_price": round(entry_price, 4),
                "exit_price": round(price, 4),
                "return": round(pnl, 6),
            })
            shares = 0.0
            position_open = False

        # Track equity
        equity = cash + shares * price
        equity_curve.append(equity)

    # If still in position at end, mark-to-market
    if position_open and len(df) > 0:
        last_price = float(df.iloc[-1]["close_f"])
        cash = shares * last_price
        pnl = (last_price - entry_price) / entry_price
        trades.append({
            "entry_date": str(entry_date),
            "exit_date": str(df.iloc[-1]["date"]),
            "entry_price": round(entry_price, 4),
            "exit_price": round(last_price, 4),
            "return": round(pnl, 6),
            "note": "open_at_end",
        })
        shares = 0.0

    final_equity = cash + shares * float(df.iloc[-1]["close_f"]) if len(df) else 1.0

    # ── metrics ──────────────────────────────────────────────────────────
    total_return = final_equity - 1.0
    days = (df.iloc[-1]["date"] - df.iloc[0]["date"]).days if len(df) > 1 else 1
    years = max(days / 365.25, 0.01)
    cagr = (final_equity ** (1.0 / years)) - 1.0 if final_equity > 0 else -1.0

    # Max drawdown from equity curve
    eq = np.array(equity_curve) if equity_curve else np.array([1.0])
    peak = np.maximum.accumulate(eq)
    drawdowns = (eq - peak) / np.where(peak > 0, peak, 1.0)
    max_dd = float(drawdowns.min())

    # Win rate
    wins = [t for t in trades if t["return"] > 0]
    win_rate = len(wins) / len(trades) if trades else 0.0

    # Simplified Sharpe (annualised return / annualised volatility of daily returns)
    if len(equity_curve) > 1:
        eq_series = pd.Series(equity_curve)
        daily_ret = eq_series.pct_change().dropna()
        ann_vol = float(daily_ret.std() * math.sqrt(252)) if len(daily_ret) > 1 else 1.0
        sharpe = cagr / ann_vol if ann_vol > 0 else 0.0
    else:
        sharpe = 0.0

    result = BacktestResult(
        ticker=ticker.upper(),
        start=str(start),
        end=str(end),
        fast_period=fast,
        slow_period=slow,
        total_return=round(total_return, 6),
        cagr=round(cagr, 6),
        max_drawdown=round(max_dd, 6),
        num_trades=len(trades),
        win_rate=round(win_rate, 4),
        sharpe_like=round(sharpe, 4),
        trades=trades,
    )
    logger.info(
        "SMA backtest %s fast=%d slow=%d | return=%.2f%% CAGR=%.2f%% DD=%.2f%% trades=%d",
        ticker, fast, slow,
        total_return * 100, cagr * 100, max_dd * 100, len(trades),
    )
    return result
