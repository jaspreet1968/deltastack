"""Intraday bar storage – minute-level Parquet files.

Layout: DATA_DIR/bars/minute/ticker={AAPL}/date={YYYY-MM-DD}/data.parquet
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from deltastack.config import get_settings

logger = logging.getLogger(__name__)


def _intraday_dir(ticker: str, bar_date: date) -> Path:
    settings = get_settings()
    return settings.intraday_dir / f"ticker={ticker.upper()}" / f"date={bar_date.isoformat()}"


def save_intraday(ticker: str, bar_date: date, df: pd.DataFrame) -> Path:
    """Save intraday bars to Parquet."""
    ticker = ticker.upper()
    dest = _intraday_dir(ticker, bar_date)
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / "data.parquet"

    if "timestamp" in df.columns:
        df = df.sort_values("timestamp").reset_index(drop=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression="snappy")
    logger.info("Saved %d intraday bars for %s %s -> %s", len(df), ticker, bar_date, path)
    return path


def load_intraday(
    ticker: str,
    bar_date: date,
    limit: int = 10_000,
    offset: int = 0,
) -> pd.DataFrame:
    """Load intraday bars from Parquet."""
    ticker = ticker.upper()
    path = _intraday_dir(ticker, bar_date) / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(f"No intraday data for {ticker} on {bar_date}")
    df = pd.read_parquet(path)
    return df.iloc[offset: offset + limit]


def intraday_exists(ticker: str, bar_date: date) -> bool:
    return (_intraday_dir(ticker.upper(), bar_date) / "data.parquet").exists()
