"""GET /data/status/{ticker} – metadata and coverage info for stored data."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from deltastack.data.storage import read_metadata

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/data", tags=["data"])


@router.get("/status/{ticker}")
def data_status(ticker: str):
    """Return metadata coverage for a ticker's stored bars."""
    meta = read_metadata(ticker)
    if meta is None:
        raise HTTPException(status_code=404, detail=f"No stored data for {ticker.upper()}")
    return meta
