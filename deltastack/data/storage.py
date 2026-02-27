"""Parquet-based storage layer for daily bar data.

Layout on disk
--------------
{DATA_DIR}/bars/day/ticker={AAPL}/data.parquet
{DATA_DIR}/metadata/{AAPL}.json          # last-updated ts, row count, date range
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from deltastack.config import get_settings
from deltastack.data.validation import validate_bars

logger = logging.getLogger(__name__)

# ── canonical schema for daily bars ──────────────────────────────────────────
DAILY_BAR_COLUMNS = [
    "date",       # date  (YYYY-MM-DD)
    "open",       # float
    "high",       # float
    "low",        # float
    "close",      # float
    "volume",     # int / float
    "vwap",       # float  (nullable)
    "trades",     # int    (nullable)
    "adjusted",   # bool   (nullable)
]


def _ticker_dir(ticker: str) -> Path:
    settings = get_settings()
    return settings.bars_dir / f"ticker={ticker.upper()}"


def _metadata_path(ticker: str) -> Path:
    settings = get_settings()
    return settings.metadata_dir / f"{ticker.upper()}.json"


# ── write ────────────────────────────────────────────────────────────────────

def save_bars(ticker: str, df: pd.DataFrame) -> Path:
    """Persist a DataFrame of daily bars as a Parquet file.

    If a file already exists the new rows are *merged* (upsert by date) so that
    the operation is idempotent.
    """
    ticker = ticker.upper()
    dest_dir = _ticker_dir(ticker)
    dest_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = dest_dir / "data.parquet"

    # Ensure column order / types
    df = _normalise(df)

    # Validate data quality (raises on hard errors, logs warnings)
    validate_bars(df, ticker=ticker)

    if parquet_path.exists():
        existing = pd.read_parquet(parquet_path)
        merged = pd.concat([existing, df]).drop_duplicates(subset=["date"], keep="last")
        merged.sort_values("date", inplace=True)
        merged.reset_index(drop=True, inplace=True)
        df = merged

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, parquet_path, compression="snappy")

    _write_metadata(ticker, df)
    logger.info("Saved %d bars for %s -> %s", len(df), ticker, parquet_path)
    return parquet_path


# ── read ─────────────────────────────────────────────────────────────────────

def load_bars(
    ticker: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
    limit: int = 10_000,
    offset: int = 0,
) -> pd.DataFrame:
    """Load daily bars from Parquet, optionally filtered by date range."""
    ticker = ticker.upper()
    parquet_path = _ticker_dir(ticker) / "data.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"No data on disk for {ticker}")

    df = pd.read_parquet(parquet_path)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    if start:
        df = df[df["date"] >= start]
    if end:
        df = df[df["date"] <= end]

    df = df.iloc[offset : offset + limit]
    return df


def ticker_exists(ticker: str) -> bool:
    return (_ticker_dir(ticker.upper()) / "data.parquet").exists()


# ── metadata ─────────────────────────────────────────────────────────────────

def _write_metadata(ticker: str, df: pd.DataFrame) -> None:
    meta_path = _metadata_path(ticker)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta = {
        "ticker": ticker.upper(),
        "rows": len(df),
        "min_date": str(df["date"].min()),
        "max_date": str(df["date"].max()),
        "updated_utc": datetime.now(timezone.utc).isoformat(),
    }
    meta_path.write_text(json.dumps(meta, indent=2))


def read_metadata(ticker: str) -> dict | None:
    meta_path = _metadata_path(ticker.upper())
    if not meta_path.exists():
        return None
    return json.loads(meta_path.read_text())


# ── helpers ──────────────────────────────────────────────────────────────────

def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the DataFrame matches the canonical schema."""
    # Make sure 'date' is a Python date (not datetime)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date

    for col in DAILY_BAR_COLUMNS:
        if col not in df.columns:
            df[col] = None

    return df[DAILY_BAR_COLUMNS].copy()
