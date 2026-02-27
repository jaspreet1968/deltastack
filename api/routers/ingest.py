"""Ingest endpoints – single ticker, batch, and universe ingestion."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from deltastack.config import get_settings
from deltastack.ingest.polygon import fetch_daily_bars

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ingest", tags=["ingest"])


# ── request / response models ────────────────────────────────────────────────

class IngestRequest(BaseModel):
    ticker: str = Field(..., examples=["AAPL"])
    start: date = Field(..., examples=["2024-01-01"])
    end: date = Field(..., examples=["2026-01-01"])
    force: bool = False


class IngestResponse(BaseModel):
    ticker: str
    rows: int
    path: str
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    skipped: bool = False


class BatchIngestRequest(BaseModel):
    tickers: List[str] = Field(..., examples=[["AAPL", "MSFT"]])
    start: date = Field(..., examples=["2024-01-01"])
    end: date = Field(..., examples=["2026-01-01"])
    force: bool = False
    max_workers: int = Field(4, ge=1, le=8)


class UniverseIngestRequest(BaseModel):
    start: date = Field(..., examples=["2024-01-01"])
    end: date = Field(..., examples=["2026-01-01"])
    force: bool = False
    max_workers: int = Field(4, ge=1, le=8)


# ── POST /ingest/daily ───────────────────────────────────────────────────────

@router.post("/daily", response_model=IngestResponse)
def ingest_daily(body: IngestRequest):
    """Download daily bars from Polygon and store as Parquet."""
    logger.info("Ingest request: %s [%s – %s] force=%s", body.ticker, body.start, body.end, body.force)
    try:
        result = fetch_daily_bars(
            ticker=body.ticker,
            start=body.start,
            end=body.end,
            force=body.force,
        )
        return IngestResponse(**result)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected error during ingestion")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── POST /ingest/batch ───────────────────────────────────────────────────────

@router.post("/batch")
def ingest_batch(body: BatchIngestRequest):
    """Ingest daily bars for multiple tickers concurrently."""
    settings = get_settings()
    workers = min(body.max_workers, settings.max_batch_workers, 8)
    logger.info(
        "Batch ingest: %d tickers [%s – %s] workers=%d force=%s",
        len(body.tickers), body.start, body.end, workers, body.force,
    )

    results = []

    def _ingest(ticker: str) -> dict:
        try:
            return fetch_daily_bars(ticker, body.start, body.end, force=body.force)
        except Exception as exc:
            logger.exception("Batch ingest failed for %s", ticker)
            return {"ticker": ticker, "error": str(exc), "rows": 0}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_ingest, t.strip().upper()): t for t in body.tickers}
        for future in as_completed(futures):
            results.append(future.result())

    return {
        "total": len(results),
        "results": results,
    }


# ── POST /ingest/universe ────────────────────────────────────────────────────

@router.post("/universe")
def ingest_universe(body: UniverseIngestRequest):
    """Ingest daily bars for all tickers listed in the universe file."""
    settings = get_settings()
    universe_path = Path(settings.universe_file)

    if not universe_path.exists():
        raise HTTPException(
            status_code=400,
            detail=(
                f"Universe file not found at {universe_path}. "
                "Create it with one ticker per line, e.g.:\n"
                "  mkdir -p config && echo 'AAPL\\nMSFT\\nGOOGL' > config/universe.txt"
            ),
        )

    tickers = [
        line.strip().upper()
        for line in universe_path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    if not tickers:
        raise HTTPException(status_code=400, detail="Universe file is empty")

    logger.info("Universe ingest: %d tickers from %s", len(tickers), universe_path)

    # Reuse batch logic
    batch_req = BatchIngestRequest(
        tickers=tickers,
        start=body.start,
        end=body.end,
        force=body.force,
        max_workers=body.max_workers,
    )
    return ingest_batch(batch_req)


# ── GET /ingest/status ────────────────────────────────────────────────────────

@router.get("/status")
def ingest_status():
    """Return the last 20 ingestion run records from DuckDB."""
    from deltastack.db.dao import list_ingestion_runs
    runs = list_ingestion_runs(limit=20)
    # Serialise timestamps
    for r in runs:
        for k in ("started_at", "ended_at"):
            if r.get(k) is not None:
                r[k] = str(r[k])
    return {"runs": runs}
