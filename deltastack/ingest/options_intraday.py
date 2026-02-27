"""Intraday options chain snapshot ingestion and storage.

Layout: DATA_DIR/options/snapshots_intraday/underlying={U}/date={D}/time={HHMM}/data.parquet
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
from deltastack.db.connection import get_db
from deltastack.ingest.options_chain import _download_snapshot, _contracts_to_df

logger = logging.getLogger(__name__)


def _snapshot_dir(underlying: str, snap_date: date, snap_time: str) -> Path:
    settings = get_settings()
    return (settings.options_intraday_dir /
            f"underlying={underlying.upper()}" /
            f"date={snap_date.isoformat()}" /
            f"time={snap_time}")


def save_intraday_snapshot(underlying: str, snap_date: date, snap_time: str, df: pd.DataFrame) -> Path:
    dest = _snapshot_dir(underlying, snap_date, snap_time)
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / "data.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression="snappy")

    # Update index
    db = get_db()
    exps = ",".join(sorted(df["expiration"].unique().tolist())) if "expiration" in df.columns else ""
    db.execute(
        "INSERT INTO options_intraday_index (underlying, snap_date, snap_time, rows_count, expirations) VALUES (?,?,?,?,?)",
        [underlying.upper(), str(snap_date), snap_time, len(df), exps],
    )
    logger.info("Saved %d intraday options for %s %s %s", len(df), underlying, snap_date, snap_time)
    return path


def load_intraday_snapshot(underlying: str, snap_date: date, snap_time: str,
                            expiration: Optional[date] = None,
                            option_type: Optional[str] = None,
                            strike_min: Optional[float] = None,
                            strike_max: Optional[float] = None) -> pd.DataFrame:
    path = _snapshot_dir(underlying.upper(), snap_date, snap_time) / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(f"No intraday snapshot for {underlying} {snap_date} {snap_time}")
    df = pd.read_parquet(path)
    if expiration and "expiration" in df.columns:
        df = df[df["expiration"] == str(expiration)]
    if option_type and "type" in df.columns:
        df = df[df["type"] == option_type.lower()]
    if strike_min is not None and "strike" in df.columns:
        df = df[pd.to_numeric(df["strike"], errors="coerce") >= strike_min]
    if strike_max is not None and "strike" in df.columns:
        df = df[pd.to_numeric(df["strike"], errors="coerce") <= strike_max]
    return df


def fetch_chain_snapshot_intraday(underlying: str, snap_date: date, snap_time: str, force: bool = False) -> dict:
    """Download intraday options snapshot from Polygon."""
    underlying = underlying.upper()
    settings = get_settings()
    api_key = settings.massive_api_key
    if not api_key:
        raise RuntimeError("MASSIVE_API_KEY is not set")

    if not force:
        path = _snapshot_dir(underlying, snap_date, snap_time) / "data.parquet"
        if path.exists():
            return {"underlying": underlying, "date": str(snap_date), "time": snap_time, "rows": 0, "skipped": True}

    contracts = _download_snapshot(underlying, snap_date, api_key)
    if not contracts:
        return {"underlying": underlying, "date": str(snap_date), "time": snap_time, "rows": 0, "skipped": False,
                "warning": "No data from Polygon"}

    df = _contracts_to_df(contracts, snap_date)
    path = save_intraday_snapshot(underlying, snap_date, snap_time, df)
    return {"underlying": underlying, "date": str(snap_date), "time": snap_time, "rows": len(df), "path": str(path)}


def list_available_times(underlying: str, snap_date: date) -> list:
    """Return list of available snapshot times for a date."""
    db = get_db()
    rows = db.execute(
        "SELECT snap_time, rows_count FROM options_intraday_index WHERE underlying=? AND snap_date=? ORDER BY snap_time",
        [underlying.upper(), str(snap_date)],
    ).fetchall()
    return [{"time": r[0], "rows": r[1]} for r in rows]
