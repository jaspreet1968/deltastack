"""Intraday data ingestion and retrieval endpoints."""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from deltastack.ingest.intraday import fetch_intraday_bars
from deltastack.data.intraday import load_intraday

logger = logging.getLogger(__name__)
router = APIRouter(tags=["intraday"])


class IntradayIngestRequest(BaseModel):
    ticker: str = Field(..., examples=["AAPL"])
    date: date = Field(..., examples=["2026-02-06"])
    timespan: str = Field("minute", examples=["minute"])
    multiplier: int = Field(5, ge=1, le=60)
    force: bool = False


@router.post("/ingest/intraday")
def ingest_intraday(body: IntradayIngestRequest):
    """Download and store intraday bars from Polygon."""
    logger.info("Intraday ingest: %s %s %s×%d", body.ticker, body.date, body.timespan, body.multiplier)
    try:
        result = fetch_intraday_bars(
            ticker=body.ticker,
            bar_date=body.date,
            timespan=body.timespan,
            multiplier=body.multiplier,
            force=body.force,
        )
        return result
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        logger.exception("Intraday ingest failed")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/intraday/{ticker}")
def get_intraday(
    ticker: str,
    date: date = Query(..., description="Bar date"),
    limit: int = Query(10_000, ge=1, le=100_000),
    offset: int = Query(0, ge=0),
):
    """Return stored intraday bars."""
    try:
        df = load_intraday(ticker, date, limit=limit, offset=offset)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No intraday data for {ticker.upper()} on {date}")

    records = df.to_dict(orient="records")
    return {
        "ticker": ticker.upper(),
        "date": str(date),
        "count": len(records),
        "bars": records,
    }
