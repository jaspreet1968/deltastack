"""Polygon.io / Massive data ingestion for daily aggregates (bars).

Uses the ``polygon-api-client`` library when available, falls back to raw
``requests`` otherwise.  All results are normalised to the canonical schema
defined in ``deltastack.data.storage``.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import List, Optional

import pandas as pd

from deltastack.config import get_settings
from deltastack.data.storage import save_bars, read_metadata

logger = logging.getLogger(__name__)

# Maximum results per Polygon REST call (free-tier safe)
_PAGE_LIMIT = 50_000


# ── public API ───────────────────────────────────────────────────────────────

def fetch_daily_bars(
    ticker: str,
    start: date,
    end: date,
    force: bool = False,
) -> dict:
    """Download daily bars from Polygon and persist to Parquet.

    Parameters
    ----------
    ticker : str
        Equity ticker symbol (e.g. ``AAPL``).
    start, end : date
        Inclusive date range.
    force : bool
        If ``True`` re-download even if data already exists for the range.

    Returns
    -------
    dict  with keys: ticker, rows, path, min_date, max_date
    """
    ticker = ticker.upper()

    # Idempotency: skip if we already cover the range (unless force)
    if not force:
        meta = read_metadata(ticker)
        if meta and meta.get("min_date") and meta.get("max_date"):
            existing_min = date.fromisoformat(str(meta["min_date"]))
            existing_max = date.fromisoformat(str(meta["max_date"]))
            if existing_min <= start and existing_max >= end:
                logger.info(
                    "Data for %s [%s – %s] already on disk; skipping (use force=True to re-download)",
                    ticker, start, end,
                )
                return {
                    "ticker": ticker,
                    "rows": meta["rows"],
                    "path": str(meta.get("path", "")),
                    "min_date": meta["min_date"],
                    "max_date": meta["max_date"],
                    "skipped": True,
                }

    df = _download_bars(ticker, start, end)
    if df.empty:
        logger.warning("Polygon returned 0 bars for %s [%s – %s]", ticker, start, end)
        return {"ticker": ticker, "rows": 0, "path": "", "min_date": None, "max_date": None, "skipped": False}

    path = save_bars(ticker, df)
    return {
        "ticker": ticker,
        "rows": len(df),
        "path": str(path),
        "min_date": str(df["date"].min()),
        "max_date": str(df["date"].max()),
        "skipped": False,
    }


def fetch_batch(
    tickers: List[str],
    start: date,
    end: date,
    force: bool = False,
) -> List[dict]:
    """Convenience wrapper – ingest multiple tickers sequentially."""
    results = []
    for t in tickers:
        try:
            res = fetch_daily_bars(t, start, end, force=force)
            results.append(res)
        except Exception as exc:
            logger.exception("Failed to ingest %s: %s", t, exc)
            results.append({"ticker": t, "error": str(exc)})
    return results


# ── internal ─────────────────────────────────────────────────────────────────

def _download_bars(ticker: str, start: date, end: date) -> pd.DataFrame:
    """Fetch aggregates from Polygon REST API v2 with retry/backoff.

    Pagination is handled via the ``next_url`` cursor that Polygon returns.
    """
    from deltastack.ingest.http_retry import get_with_retry

    settings = get_settings()
    api_key = settings.massive_api_key
    if not api_key:
        raise RuntimeError(
            "MASSIVE_API_KEY is not set.  Export it or add it to .env."
        )

    all_results: list[dict] = []
    url: Optional[str] = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day"
        f"/{start.isoformat()}/{end.isoformat()}"
        f"?adjusted=true&sort=asc&limit={_PAGE_LIMIT}"
    )

    while url:
        logger.debug("GET %s", url)
        resp = get_with_retry(url, params={"apiKey": api_key})
        resp.raise_for_status()
        body = resp.json()

        results = body.get("results") or []
        all_results.extend(results)
        logger.info(
            "Fetched %d bars for %s (page total: %d)",
            len(results), ticker, len(all_results),
        )

        # Polygon pagination
        url = body.get("next_url")
        if url and "apiKey" not in url:
            url = f"{url}&apiKey={api_key}"

    if not all_results:
        return pd.DataFrame()

    return _results_to_df(all_results)


def _results_to_df(results: list[dict]) -> pd.DataFrame:
    """Convert raw Polygon aggregate results to a normalised DataFrame."""
    rows = []
    for r in results:
        # Polygon 't' field is epoch-ms
        ts_ms = r.get("t")
        bar_date = pd.Timestamp(ts_ms, unit="ms", tz="UTC").date() if ts_ms else None
        rows.append(
            {
                "date": bar_date,
                "open": r.get("o"),
                "high": r.get("h"),
                "low": r.get("l"),
                "close": r.get("c"),
                "volume": r.get("v"),
                "vwap": r.get("vw"),
                "trades": r.get("n"),
                "adjusted": True,  # we request adjusted=true
            }
        )
    return pd.DataFrame(rows)
