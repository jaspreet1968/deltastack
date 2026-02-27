"""Intraday bar ingestion from Polygon REST API."""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import pandas as pd

from deltastack.config import get_settings
from deltastack.data.intraday import save_intraday, intraday_exists
from deltastack.ingest.http_retry import get_with_retry

logger = logging.getLogger(__name__)


def fetch_intraday_bars(
    ticker: str,
    bar_date: date,
    timespan: str = "minute",
    multiplier: int = 5,
    force: bool = False,
) -> dict:
    """Download intraday bars from Polygon and store as Parquet."""
    ticker = ticker.upper()

    if not force and intraday_exists(ticker, bar_date):
        logger.info("Intraday data for %s %s already exists; skipping", ticker, bar_date)
        return {"ticker": ticker, "date": str(bar_date), "rows": 0, "skipped": True}

    settings = get_settings()
    api_key = settings.massive_api_key
    if not api_key:
        raise RuntimeError("MASSIVE_API_KEY is not set")

    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}"
        f"/{bar_date.isoformat()}/{bar_date.isoformat()}"
        f"?adjusted=true&sort=asc&limit=50000"
    )

    resp = get_with_retry(url, params={"apiKey": api_key})
    resp.raise_for_status()
    body = resp.json()
    results = body.get("results") or []

    if not results:
        return {"ticker": ticker, "date": str(bar_date), "rows": 0, "skipped": False}

    rows = []
    for r in results:
        ts_ms = r.get("t")
        rows.append({
            "timestamp": pd.Timestamp(ts_ms, unit="ms", tz="UTC").isoformat() if ts_ms else None,
            "open": r.get("o"),
            "high": r.get("h"),
            "low": r.get("l"),
            "close": r.get("c"),
            "volume": r.get("v"),
            "vwap": r.get("vw"),
            "trades": r.get("n"),
        })

    df = pd.DataFrame(rows)
    path = save_intraday(ticker, bar_date, df)

    return {
        "ticker": ticker,
        "date": str(bar_date),
        "rows": len(df),
        "path": str(path),
        "skipped": False,
    }
