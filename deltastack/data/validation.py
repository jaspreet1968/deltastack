"""Data quality checks for daily bar DataFrames.

Called automatically during ingestion to ensure stored data is clean.
Logs warnings for suspicious gaps but only raises on hard schema violations.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import List

import pandas as pd

from deltastack.config import get_settings

logger = logging.getLogger(__name__)

REQUIRED_OHLCV = ["date", "open", "high", "low", "close", "volume"]


def validate_bars(df: pd.DataFrame, ticker: str = "") -> List[str]:
    """Validate a daily-bar DataFrame.  Returns a list of warning strings.

    Raises
    ------
    ValueError
        If required OHLCV columns are missing or entirely null.
    """
    warnings: List[str] = []
    prefix = f"[{ticker}] " if ticker else ""
    settings = get_settings()

    # ── 1. Required columns present ──────────────────────────────────────
    missing = [c for c in REQUIRED_OHLCV if c not in df.columns]
    if missing:
        raise ValueError(f"{prefix}Missing required columns: {missing}")

    # ── 2. No entirely-null OHLCV columns ────────────────────────────────
    for col in REQUIRED_OHLCV:
        if df[col].isna().all():
            raise ValueError(f"{prefix}Column '{col}' is entirely null")

    # ── 3. Date monotonicity ─────────────────────────────────────────────
    dates = pd.to_datetime(df["date"])
    if not dates.is_monotonic_increasing:
        warnings.append(f"{prefix}Dates are not strictly monotonic – will be sorted")

    # ── 4. Duplicate dates ───────────────────────────────────────────────
    dups = dates.duplicated().sum()
    if dups > 0:
        warnings.append(f"{prefix}{dups} duplicate date(s) detected – will be deduped")

    # ── 5. Calendar-day gaps ─────────────────────────────────────────────
    if len(dates) >= 2:
        diffs = dates.diff().dropna()
        max_gap = diffs.max()
        threshold = timedelta(days=settings.gap_warn_days)
        if max_gap > threshold:
            gap_start = dates[diffs.idxmax() - 1] if diffs.idxmax() > 0 else dates.iloc[0]
            warnings.append(
                f"{prefix}Largest calendar-day gap is {max_gap.days}d "
                f"(threshold {settings.gap_warn_days}d) near {gap_start.date()}"
            )

    for w in warnings:
        logger.warning(w)

    return warnings
