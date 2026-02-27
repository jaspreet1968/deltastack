"""GET /prices/{ticker} – return stored daily bars with read caching."""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from deltastack.data.cache import get_bars_cache, make_cache_key
from deltastack.data.storage import load_bars

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/prices", tags=["prices"])


@router.get("/{ticker}")
def get_prices(
    ticker: str,
    start: Optional[date] = Query(None, description="Start date (inclusive)"),
    end: Optional[date] = Query(None, description="End date (inclusive)"),
    limit: int = Query(10_000, ge=1, le=100_000),
    offset: int = Query(0, ge=0),
):
    """Return daily bars for a ticker from local Parquet storage."""
    cache = get_bars_cache()
    key = make_cache_key("bars", ticker.upper(), start, end, limit, offset)
    cached = cache.get(key)
    if cached is not None:
        return cached

    try:
        df = load_bars(ticker, start=start, end=end, limit=limit, offset=offset)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No data found for {ticker.upper()}")

    records = df.to_dict(orient="records")
    for r in records:
        if r.get("date"):
            r["date"] = str(r["date"])

    result = {
        "ticker": ticker.upper(),
        "count": len(records),
        "offset": offset,
        "bars": records,
    }
    cache.put(key, result)
    return result
