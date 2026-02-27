"""Signals endpoints – SMA signal, run_universe, latest."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import date
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from deltastack.config import get_settings
from deltastack.data.storage import load_bars, ticker_exists
from deltastack.db.dao import insert_signal
from deltastack.db.connection import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/signals", tags=["signals"])


class SMASignalRequest(BaseModel):
    ticker: str = Field(..., examples=["AAPL"])
    fast: int = Field(10, ge=2, le=200)
    slow: int = Field(30, ge=5, le=500)


@router.post("/sma")
def sma_signal(body: SMASignalRequest):
    """Compute the latest SMA crossover signal from stored data.

    Returns BUY / SELL / HOLD with the as-of date and a short explanation.
    """
    if body.fast >= body.slow:
        raise HTTPException(status_code=400, detail=f"fast ({body.fast}) must be < slow ({body.slow})")

    try:
        df = load_bars(body.ticker, limit=100_000)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"No price data for {body.ticker.upper()}. Ingest first via POST /ingest/daily.",
        )

    if len(df) < body.slow + 1:
        raise HTTPException(
            status_code=400,
            detail=f"Not enough data ({len(df)} bars) to compute slow SMA ({body.slow}).",
        )

    df = df.sort_values("date").reset_index(drop=True)
    df["close_f"] = df["close"].astype(float)
    df["sma_fast"] = df["close_f"].rolling(window=body.fast).mean()
    df["sma_slow"] = df["close_f"].rolling(window=body.slow).mean()

    latest = df.dropna(subset=["sma_fast", "sma_slow"]).iloc[-1]
    prev = df.dropna(subset=["sma_fast", "sma_slow"]).iloc[-2] if len(df.dropna(subset=["sma_fast", "sma_slow"])) >= 2 else None

    fast_val = float(latest["sma_fast"])
    slow_val = float(latest["sma_slow"])
    as_of = str(latest["date"])

    # Determine signal
    if prev is not None:
        prev_fast = float(prev["sma_fast"])
        prev_slow = float(prev["sma_slow"])
        if prev_fast <= prev_slow and fast_val > slow_val:
            signal = "BUY"
            reason = f"SMA({body.fast}) just crossed above SMA({body.slow})"
        elif prev_fast >= prev_slow and fast_val < slow_val:
            signal = "SELL"
            reason = f"SMA({body.fast}) just crossed below SMA({body.slow})"
        elif fast_val > slow_val:
            signal = "HOLD"
            reason = f"SMA({body.fast}) ({fast_val:.2f}) > SMA({body.slow}) ({slow_val:.2f}) – bullish trend"
        else:
            signal = "HOLD"
            reason = f"SMA({body.fast}) ({fast_val:.2f}) <= SMA({body.slow}) ({slow_val:.2f}) – bearish trend"
    else:
        signal = "HOLD"
        reason = "Insufficient history for crossover detection"

    return {
        "ticker": body.ticker.upper(),
        "signal": signal,
        "reason": reason,
        "as_of": as_of,
        "sma_fast": round(fast_val, 4),
        "sma_slow": round(slow_val, 4),
        "close": round(float(latest["close_f"]), 4),
    }


# ── POST /signals/run_universe ───────────────────────────────────────────────

@router.post("/run_universe")
def run_universe_signals():
    """Compute SMA signals for all tickers in universe.txt and persist to DB."""
    settings = get_settings()
    universe_path = Path(settings.universe_file)
    if not universe_path.exists():
        raise HTTPException(status_code=400, detail=f"Universe file not found: {universe_path}")

    tickers = [
        line.strip().upper()
        for line in universe_path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    batch_id = uuid.uuid4().hex[:12]
    fast, slow = 10, 30
    results = []

    for ticker in tickers:
        if not ticker_exists(ticker):
            results.append({"ticker": ticker, "signal": None, "reason": "no_data"})
            continue
        try:
            df = load_bars(ticker, limit=100_000)
            if len(df) < slow + 1:
                results.append({"ticker": ticker, "signal": None, "reason": "insufficient_data"})
                continue

            df = df.sort_values("date").reset_index(drop=True)
            df["close_f"] = df["close"].astype(float)
            df["sma_fast"] = df["close_f"].rolling(window=fast).mean()
            df["sma_slow"] = df["close_f"].rolling(window=slow).mean()
            df = df.dropna(subset=["sma_fast", "sma_slow"])

            if len(df) < 2:
                results.append({"ticker": ticker, "signal": None, "reason": "insufficient_sma_data"})
                continue

            latest_row = df.iloc[-1]
            prev_row = df.iloc[-2]
            fv = float(latest_row["sma_fast"])
            sv = float(latest_row["sma_slow"])

            if float(prev_row["sma_fast"]) <= float(prev_row["sma_slow"]) and fv > sv:
                sig = "BUY"
            elif float(prev_row["sma_fast"]) >= float(prev_row["sma_slow"]) and fv < sv:
                sig = "SELL"
            else:
                sig = "HOLD"

            insert_signal(
                strategy=f"sma_{fast}_{slow}",
                ticker=ticker,
                signal=sig,
                as_of=str(latest_row["date"]),
                meta={"batch_id": batch_id, "fast": fv, "slow": sv},
            )
            results.append({"ticker": ticker, "signal": sig, "as_of": str(latest_row["date"])})
        except Exception as exc:
            results.append({"ticker": ticker, "signal": None, "reason": str(exc)})

    return {"batch_id": batch_id, "count": len(results), "results": results}


# ── GET /signals/latest ──────────────────────────────────────────────────────

@router.get("/latest")
def latest_signal(ticker: str = Query(..., description="Ticker symbol")):
    """Return the most recent signal for a ticker."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM signals WHERE ticker = ? ORDER BY created_at DESC LIMIT 1",
        [ticker.upper()],
    ).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No signals found for {ticker.upper()}")
    cols = [d[0] for d in db.description]
    row = dict(zip(cols, rows[0]))
    row["created_at"] = str(row["created_at"]) if row.get("created_at") else None
    if isinstance(row.get("meta_json"), str):
        try:
            row["meta_json"] = json.loads(row["meta_json"])
        except (json.JSONDecodeError, TypeError):
            pass
    return row
