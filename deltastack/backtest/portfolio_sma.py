"""Portfolio-aware SMA crossover backtest.

Supports multiple tickers, cash management, position sizing, commissions,
and slippage.  Results are persisted to DuckDB.
"""

from __future__ import annotations

import logging
import math
import uuid
from dataclasses import dataclass, field, asdict
from datetime import date
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from deltastack.backtest.base import BacktestResult, Strategy
from deltastack.data.storage import load_bars
from deltastack.db.dao import insert_backtest_run, insert_trade

logger = logging.getLogger(__name__)


@dataclass
class PortfolioConfig:
    tickers: List[str]
    start: date
    end: date
    fast: int = 10
    slow: int = 30
    initial_cash: float = 100_000.0
    max_positions: int = 3
    risk_per_trade: float = 0.02       # fraction of equity risked per trade
    commission_per_trade: float = 1.0
    slippage_bps: float = 2.0          # basis points


def run_portfolio_sma_backtest(cfg: PortfolioConfig) -> dict:
    """Execute a multi-ticker SMA crossover backtest with position sizing."""
    run_id = uuid.uuid4().hex[:16]
    logger.info("Portfolio SMA backtest run_id=%s tickers=%s", run_id, cfg.tickers)

    # ── load & prepare data ──────────────────────────────────────────────
    frames: Dict[str, pd.DataFrame] = {}
    for ticker in cfg.tickers:
        try:
            df = load_bars(ticker, start=cfg.start, end=cfg.end, limit=100_000)
            if df.empty:
                logger.warning("No data for %s – skipping", ticker)
                continue
            df = df.sort_values("date").reset_index(drop=True)
            df["close_f"] = df["close"].astype(float)
            df["sma_fast"] = df["close_f"].rolling(window=cfg.fast).mean()
            df["sma_slow"] = df["close_f"].rolling(window=cfg.slow).mean()
            df = df.dropna(subset=["sma_fast", "sma_slow"]).reset_index(drop=True)
            if len(df) >= 2:
                frames[ticker] = df
        except FileNotFoundError:
            logger.warning("No stored data for %s", ticker)

    if not frames:
        raise ValueError("No usable data for any ticker in the request")

    # ── build unified date index ─────────────────────────────────────────
    all_dates = sorted(set().union(*(set(df["date"].tolist()) for df in frames.values())))

    # ── simulation state ─────────────────────────────────────────────────
    cash = cfg.initial_cash
    positions: Dict[str, dict] = {}   # ticker -> {qty, entry_price, entry_date}
    all_trades: List[dict] = []
    equity_curve: List[dict] = []
    slippage_mult = 1.0 + cfg.slippage_bps / 10_000.0

    def _price_on(ticker: str, d: date) -> Optional[float]:
        df = frames.get(ticker)
        if df is None:
            return None
        row = df[df["date"] == d]
        return float(row.iloc[0]["close_f"]) if not row.empty else None

    def _signal_on(ticker: str, d: date) -> Optional[int]:
        """Return 1 (buy), -1 (sell), 0 (no change) based on SMA cross."""
        df = frames.get(ticker)
        if df is None:
            return None
        idx = df.index[df["date"] == d]
        if len(idx) == 0 or idx[0] == 0:
            return 0
        i = idx[0]
        prev_fast = float(df.at[i - 1, "sma_fast"])
        prev_slow = float(df.at[i - 1, "sma_slow"])
        curr_fast = float(df.at[i, "sma_fast"])
        curr_slow = float(df.at[i, "sma_slow"])
        if prev_fast <= prev_slow and curr_fast > curr_slow:
            return 1   # bullish cross
        if prev_fast >= prev_slow and curr_fast < curr_slow:
            return -1  # bearish cross
        return 0

    # ── day-by-day simulation ────────────────────────────────────────────
    for d in all_dates:
        # Check signals
        for ticker in list(frames.keys()):
            sig = _signal_on(ticker, d)
            price = _price_on(ticker, d)
            if sig is None or price is None:
                continue

            if sig == 1 and ticker not in positions and len(positions) < cfg.max_positions:
                # BUY
                equity = cash + sum(
                    pos["qty"] * (_price_on(t, d) or pos["entry_price"])
                    for t, pos in positions.items()
                )
                risk_amount = equity * cfg.risk_per_trade
                fill_price = price * slippage_mult
                qty = risk_amount / fill_price if fill_price > 0 else 0
                cost = qty * fill_price + cfg.commission_per_trade
                if cost <= cash and qty > 0:
                    cash -= cost
                    positions[ticker] = {
                        "qty": qty,
                        "entry_price": fill_price,
                        "entry_date": str(d),
                    }

            elif sig == -1 and ticker in positions:
                # SELL
                pos = positions.pop(ticker)
                fill_price = price * (2.0 - slippage_mult)  # adverse slippage on sell
                proceeds = pos["qty"] * fill_price - cfg.commission_per_trade
                cash += proceeds
                pnl = (fill_price - pos["entry_price"]) * pos["qty"] - 2 * cfg.commission_per_trade
                trade = {
                    "ticker": ticker,
                    "side": "SELL",
                    "qty": round(pos["qty"], 6),
                    "entry_time": pos["entry_date"],
                    "entry_price": round(pos["entry_price"], 4),
                    "exit_time": str(d),
                    "exit_price": round(fill_price, 4),
                    "pnl": round(pnl, 2),
                }
                all_trades.append(trade)

        # Mark-to-market equity
        mtm = cash + sum(
            pos["qty"] * (_price_on(t, d) or pos["entry_price"])
            for t, pos in positions.items()
        )
        equity_curve.append({"date": str(d), "equity": round(mtm, 2)})

    # ── close remaining positions at end ─────────────────────────────────
    last_date = all_dates[-1] if all_dates else cfg.end
    for ticker, pos in list(positions.items()):
        price = _price_on(ticker, last_date) or pos["entry_price"]
        fill_price = price * (2.0 - slippage_mult)
        proceeds = pos["qty"] * fill_price - cfg.commission_per_trade
        cash += proceeds
        pnl = (fill_price - pos["entry_price"]) * pos["qty"] - 2 * cfg.commission_per_trade
        all_trades.append({
            "ticker": ticker,
            "side": "SELL",
            "qty": round(pos["qty"], 6),
            "entry_time": pos["entry_date"],
            "entry_price": round(pos["entry_price"], 4),
            "exit_time": str(last_date),
            "exit_price": round(fill_price, 4),
            "pnl": round(pnl, 2),
            "note": "closed_at_end",
        })
    positions.clear()

    # ── compute metrics ──────────────────────────────────────────────────
    final_equity = cash
    total_return = (final_equity - cfg.initial_cash) / cfg.initial_cash
    days = (all_dates[-1] - all_dates[0]).days if len(all_dates) > 1 else 1
    years = max(days / 365.25, 0.01)
    cagr = (final_equity / cfg.initial_cash) ** (1.0 / years) - 1.0 if final_equity > 0 else -1.0

    eq_values = [e["equity"] for e in equity_curve]
    eq_arr = np.array(eq_values) if eq_values else np.array([cfg.initial_cash])
    peak = np.maximum.accumulate(eq_arr)
    dd = (eq_arr - peak) / np.where(peak > 0, peak, 1.0)
    max_dd = float(dd.min())

    wins = [t for t in all_trades if t["pnl"] > 0]
    win_rate = len(wins) / len(all_trades) if all_trades else 0.0

    if len(eq_values) > 1:
        daily_ret = pd.Series(eq_values).pct_change().dropna()
        ann_vol = float(daily_ret.std() * math.sqrt(252)) if len(daily_ret) > 1 else 1.0
        sharpe = cagr / ann_vol if ann_vol > 0 else 0.0
    else:
        sharpe = 0.0

    metrics = {
        "total_return": round(total_return, 6),
        "cagr": round(cagr, 6),
        "max_drawdown": round(max_dd, 6),
        "num_trades": len(all_trades),
        "win_rate": round(win_rate, 4),
        "sharpe_like": round(sharpe, 4),
        "final_equity": round(final_equity, 2),
        "initial_cash": cfg.initial_cash,
    }

    # ── persist to DB ────────────────────────────────────────────────────
    try:
        insert_backtest_run(
            run_id=run_id,
            strategy="portfolio_sma",
            tickers=",".join(cfg.tickers),
            params={
                "fast": cfg.fast,
                "slow": cfg.slow,
                "max_positions": cfg.max_positions,
                "risk_per_trade": cfg.risk_per_trade,
                "commission_per_trade": cfg.commission_per_trade,
                "slippage_bps": cfg.slippage_bps,
            },
            dt_start=str(cfg.start),
            dt_end=str(cfg.end),
            metrics=metrics,
        )
        for t in all_trades:
            insert_trade(
                run_id=run_id,
                ticker=t["ticker"],
                side=t["side"],
                qty=t["qty"],
                entry_time=t.get("entry_time", ""),
                entry_price=t.get("entry_price", 0),
                exit_time=t.get("exit_time", ""),
                exit_price=t.get("exit_price", 0),
                pnl=t.get("pnl", 0),
                meta={"note": t.get("note", "")},
            )
    except Exception:
        logger.exception("Failed to persist backtest run %s to DB", run_id)

    logger.info(
        "Portfolio SMA run_id=%s return=%.2f%% CAGR=%.2f%% MDD=%.2f%% trades=%d",
        run_id, total_return * 100, cagr * 100, max_dd * 100, len(all_trades),
    )

    return {
        "run_id": run_id,
        "strategy": "portfolio_sma",
        "tickers": cfg.tickers,
        "metrics": metrics,
        "trades": all_trades,
        "equity_curve_length": len(equity_curve),
        "equity_curve": equity_curve,  # caller can trim via include_curve flag
    }
