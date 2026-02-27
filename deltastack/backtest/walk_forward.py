"""Walk-Forward Validation (WFA) for SMA strategies.

Splits data into rolling train/test windows, optimizes params on train,
evaluates on test. Reduces overfitting vs single-window backtest.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, timedelta
from itertools import product
from typing import Dict, List

import pandas as pd

from deltastack.backtest.sma import run_sma_backtest
from deltastack.data.storage import load_bars
from deltastack.db.connection import get_db

logger = logging.getLogger(__name__)


def run_walk_forward_sma(
    ticker: str,
    start: date,
    end: date,
    train_window_days: int = 504,
    test_window_days: int = 63,
    param_grid: Dict[str, List[int]] | None = None,
) -> dict:
    """Execute walk-forward validation for SMA strategy."""
    run_id = uuid.uuid4().hex[:16]

    if param_grid is None:
        param_grid = {"fast": [5, 10, 20], "slow": [30, 50, 100]}

    fast_values = param_grid.get("fast", [10])
    slow_values = param_grid.get("slow", [30])

    # Load all data once
    df = load_bars(ticker, start=start, end=end, limit=100_000)
    if df.empty:
        raise ValueError(f"No bars for {ticker} in [{start}, {end}]")

    df = df.sort_values("date").reset_index(drop=True)
    all_dates = list(df["date"])

    if len(all_dates) < train_window_days + test_window_days:
        raise ValueError(
            f"Need at least {train_window_days + test_window_days} days, have {len(all_dates)}"
        )

    # ── Build folds ──────────────────────────────────────────────────────
    folds = []
    fold_num = 0
    cursor = 0

    while cursor + train_window_days + test_window_days <= len(all_dates):
        train_start = all_dates[cursor]
        train_end = all_dates[cursor + train_window_days - 1]
        test_start = all_dates[cursor + train_window_days]
        test_end_idx = min(cursor + train_window_days + test_window_days - 1, len(all_dates) - 1)
        test_end = all_dates[test_end_idx]

        # ── Grid search on train window ──────────────────────────────────
        best_sharpe = -999
        best_params = {"fast": fast_values[0], "slow": slow_values[0]}

        for fast, slow in product(fast_values, slow_values):
            if fast >= slow:
                continue
            try:
                result = run_sma_backtest(ticker, train_start, train_end, fast=fast, slow=slow)
                metric = result.sharpe_like
                if metric > best_sharpe:
                    best_sharpe = metric
                    best_params = {"fast": fast, "slow": slow}
            except (ValueError, FileNotFoundError):
                continue

        # ── Evaluate on test window ──────────────────────────────────────
        try:
            test_result = run_sma_backtest(
                ticker, test_start, test_end,
                fast=best_params["fast"], slow=best_params["slow"],
            )
            test_sharpe = test_result.sharpe_like
        except (ValueError, FileNotFoundError):
            test_sharpe = 0.0

        folds.append({
            "fold_num": fold_num,
            "train_start": str(train_start),
            "train_end": str(train_end),
            "test_start": str(test_start),
            "test_end": str(test_end),
            "chosen_params": best_params,
            "train_sharpe": round(best_sharpe, 4),
            "test_sharpe": round(test_sharpe, 4),
        })

        fold_num += 1
        cursor += test_window_days  # slide by test window

    if not folds:
        raise ValueError("No valid folds could be created")

    # ── Aggregate metrics ────────────────────────────────────────────────
    avg_train = sum(f["train_sharpe"] for f in folds) / len(folds)
    avg_test = sum(f["test_sharpe"] for f in folds) / len(folds)

    metrics = {
        "num_folds": len(folds),
        "avg_train_sharpe": round(avg_train, 4),
        "avg_test_sharpe": round(avg_test, 4),
        "train_test_ratio": round(avg_test / avg_train, 4) if avg_train != 0 else 0,
    }

    # ── Persist to DB ────────────────────────────────────────────────────
    try:
        db = get_db()
        import json
        db.execute(
            "INSERT INTO walk_forward_runs (run_id, ticker, dt_start, dt_end, params_json, metrics_json) VALUES (?,?,?,?,?,?)",
            [run_id, ticker.upper(), str(start), str(end),
             json.dumps({"param_grid": param_grid, "train_days": train_window_days, "test_days": test_window_days}),
             json.dumps(metrics)],
        )
        for f in folds:
            db.execute(
                """INSERT INTO walk_forward_folds
                   (run_id, fold_num, train_start, train_end, test_start, test_end, chosen_params, train_metric, test_metric)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                [run_id, f["fold_num"], f["train_start"], f["train_end"],
                 f["test_start"], f["test_end"], json.dumps(f["chosen_params"]),
                 f["train_sharpe"], f["test_sharpe"]],
            )
    except Exception:
        logger.exception("Failed to persist WFA run %s", run_id)

    logger.info("WFA run_id=%s folds=%d avg_train=%.4f avg_test=%.4f",
                run_id, len(folds), avg_train, avg_test)

    return {
        "run_id": run_id,
        "ticker": ticker.upper(),
        "folds": folds,
        "metrics": metrics,
    }
