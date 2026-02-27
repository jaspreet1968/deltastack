"""Buy-and-Hold strategy – the simplest possible benchmark.

Buy at first bar close, hold until last bar close.
"""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from deltastack.backtest.base import BacktestResult, Strategy
from deltastack.data.storage import load_bars

logger = logging.getLogger(__name__)


class BuyHoldStrategy(Strategy):
    name = "buy_hold"

    def run(self, df: pd.DataFrame, **params) -> BacktestResult:
        df = df.sort_values("date").reset_index(drop=True)
        df["close_f"] = df["close"].astype(float)

        if len(df) < 2:
            raise ValueError("Need at least 2 bars for buy-and-hold")

        entry = float(df.iloc[0]["close_f"])
        exit_ = float(df.iloc[-1]["close_f"])
        total_return = (exit_ - entry) / entry

        days = (df.iloc[-1]["date"] - df.iloc[0]["date"]).days
        cagr = self.compute_cagr(1.0, 1.0 + total_return, days)

        equity_curve = (df["close_f"] / entry).tolist()
        max_dd = self.compute_max_drawdown(equity_curve)
        sharpe = self.compute_sharpe(cagr, equity_curve)

        trades = [
            {
                "entry_date": str(df.iloc[0]["date"]),
                "exit_date": str(df.iloc[-1]["date"]),
                "entry_price": round(entry, 4),
                "exit_price": round(exit_, 4),
                "return": round(total_return, 6),
            }
        ]

        ticker = params.get("ticker", "")
        result = BacktestResult(
            strategy=self.name,
            ticker=ticker.upper(),
            start=str(df.iloc[0]["date"]),
            end=str(df.iloc[-1]["date"]),
            params={},
            total_return=round(total_return, 6),
            cagr=round(cagr, 6),
            max_drawdown=round(max_dd, 6),
            num_trades=1,
            win_rate=1.0 if total_return > 0 else 0.0,
            sharpe_like=round(sharpe, 4),
            trades=trades,
        )
        logger.info(
            "Buy-hold %s | return=%.2f%% CAGR=%.2f%% DD=%.2f%%",
            ticker, total_return * 100, cagr * 100, max_dd * 100,
        )
        return result


def run_buy_hold_backtest(
    ticker: str,
    start: date,
    end: date,
) -> BacktestResult:
    """Convenience function matching the pattern of ``run_sma_backtest``."""
    df = load_bars(ticker, start=start, end=end, limit=100_000)
    if df.empty:
        raise ValueError(f"No bars loaded for {ticker} in [{start}, {end}]")
    strategy = BuyHoldStrategy()
    return strategy.run(df, ticker=ticker)
