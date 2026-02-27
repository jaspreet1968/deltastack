"""Options chain endpoints – snapshot ingestion and retrieval + greeks."""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from deltastack.ingest.options_chain import fetch_chain_snapshot, load_chain
from deltastack.options.greeks import compute_greeks, implied_vol
from deltastack.backtest.credit_spread import CreditSpreadConfig, run_credit_spread_backtest
from deltastack.backtest.zero_dte import ZeroDTEConfig, run_0dte_backtest
from deltastack.ingest.options_intraday import (
    fetch_chain_snapshot_intraday, load_intraday_snapshot, list_available_times,
)
from deltastack.config import get_settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/options", tags=["options"])


# ── models ───────────────────────────────────────────────────────────────────

class SnapshotRequest(BaseModel):
    underlying: str = Field(..., examples=["SPY"])
    as_of: date = Field(..., examples=["2026-02-06"])


class GreeksRequest(BaseModel):
    spot: float = Field(..., gt=0, examples=[450.0])
    strike: float = Field(..., gt=0, examples=[460.0])
    tte_years: float = Field(..., gt=0, examples=[0.08])  # time to expiry in years
    iv: float = Field(..., gt=0, examples=[0.25])
    option_type: str = Field("call", examples=["call"])
    risk_free_rate: Optional[float] = None  # uses config default if None


# ── POST /options/chain/snapshot ─────────────────────────────────────────────

@router.post("/chain/snapshot")
def ingest_snapshot(body: SnapshotRequest):
    """Download and store an options chain snapshot from Polygon."""
    logger.info("Options snapshot request: %s as_of=%s", body.underlying, body.as_of)
    try:
        result = fetch_chain_snapshot(body.underlying, body.as_of)
        return result
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Options snapshot failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /options/chain/{underlying} ──────────────────────────────────────────

@router.get("/chain/{underlying}")
def get_chain(
    underlying: str,
    as_of: date = Query(..., description="Snapshot date"),
    expiration: Optional[date] = Query(None),
    type: Optional[str] = Query(None, description="call or put"),
    strike_min: Optional[float] = Query(None),
    strike_max: Optional[float] = Query(None),
    limit: int = Query(500, ge=1, le=5000),
):
    """Retrieve a stored options chain snapshot with optional filters."""
    try:
        df = load_chain(
            underlying,
            as_of=as_of,
            expiration=expiration,
            option_type=type,
            strike_min=strike_min,
            strike_max=strike_max,
        )
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"No options snapshot for {underlying.upper()} as_of={as_of}. Ingest first via POST /options/chain/snapshot.",
        )

    records = df.head(limit).to_dict(orient="records")
    return {
        "underlying": underlying.upper(),
        "as_of": str(as_of),
        "count": len(records),
        "contracts": records,
    }


# ── POST /options/greeks ────────────────────────────────────────────────────

@router.post("/greeks")
def calc_greeks(body: GreeksRequest):
    """Compute Black-Scholes greeks for a European option."""
    settings = get_settings()
    r = body.risk_free_rate if body.risk_free_rate is not None else settings.risk_free_rate

    result = compute_greeks(
        S=body.spot,
        K=body.strike,
        T=body.tte_years,
        r=r,
        sigma=body.iv,
        option_type=body.option_type,
    )
    result["inputs"] = {
        "spot": body.spot,
        "strike": body.strike,
        "tte_years": body.tte_years,
        "iv": body.iv,
        "option_type": body.option_type,
        "risk_free_rate": r,
    }
    return result


# ── POST /options/backtest/credit_spread ─────────────────────────────────────

class CreditSpreadRequest(BaseModel):
    underlying: str = Field(..., examples=["SPY"])
    as_of: date = Field(..., examples=["2026-02-06"])
    spread_type: str = Field("bull_put", examples=["bull_put"])
    dte: int = Field(30, ge=5, le=365)
    spread_width: float = Field(5, gt=0)
    target_delta_short: float = Field(0.20, gt=0, le=0.50)
    min_volume: int = Field(50, ge=0)
    max_bid_ask_pct: float = Field(0.25, gt=0)
    contracts: int = Field(1, ge=1, le=100)
    slippage_pct: Optional[float] = None
    exit_profit_take_pct: float = Field(0.50, gt=0)
    exit_stop_loss_pct: float = Field(2.0, gt=0)
    exit_dte_close: int = Field(5, ge=0)


@router.post("/backtest/credit_spread")
def backtest_credit_spread(body: CreditSpreadRequest):
    """Backtest a credit spread using stored options snapshot."""
    logger.info("Options backtest: %s %s as_of=%s", body.spread_type, body.underlying, body.as_of)
    try:
        cfg = CreditSpreadConfig(
            underlying=body.underlying,
            as_of=body.as_of,
            spread_type=body.spread_type,
            dte=body.dte,
            spread_width=body.spread_width,
            target_delta_short=body.target_delta_short,
            min_volume=body.min_volume,
            max_bid_ask_pct=body.max_bid_ask_pct,
            contracts=body.contracts,
            slippage_pct=body.slippage_pct,
            exit_profit_take_pct=body.exit_profit_take_pct,
            exit_stop_loss_pct=body.exit_stop_loss_pct,
            exit_dte_close=body.exit_dte_close,
        )
        return run_credit_spread_backtest(cfg)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Options backtest failed")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /options/chain/snapshot_intraday ────────────────────────────────────

class IntradaySnapshotRequest(BaseModel):
    underlying: str = Field(..., examples=["SPY"])
    date: date = Field(..., examples=["2026-02-06"])
    time: str = Field(..., examples=["1035"])
    force: bool = False


@router.get("/snapshots_intraday/status")
def intraday_snapshot_status(
    underlying: str = Query("QQQ"),
    date: date = Query(...),
):
    """Return captured intraday snapshot times and gaps for a date."""
    from deltastack.db.connection import get_db
    db = get_db()
    rows = db.execute(
        "SELECT snap_time, status, rows_count FROM options_snapshot_runs WHERE underlying=? AND snap_date=? ORDER BY snap_time",
        [underlying.upper(), str(date)],
    ).fetchall()

    captured = [{"time": r[0], "status": r[1], "rows": r[2]} for r in rows]
    times = [r[0] for r in rows]

    # Find gaps (expected every 5 min from 0930-1600)
    gaps = []
    if times:
        for h in range(9, 16):
            for m in range(0, 60, 5):
                t = f"{h:02d}{m:02d}"
                if "0930" <= t <= "1600" and t not in times:
                    gaps.append(t)

    return {
        "underlying": underlying.upper(),
        "date": str(date),
        "captured_count": len(captured),
        "captured": captured,
        "gaps": gaps,
        "last_captured": times[-1] if times else None,
    }


@router.post("/chain/snapshot_intraday")
def ingest_intraday_snapshot(body: IntradaySnapshotRequest):
    """Download and store an intraday options chain snapshot."""
    try:
        return fetch_chain_snapshot_intraday(body.underlying, body.date, body.time, body.force)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /options/chain_intraday/{underlying} ─────────────────────────────────

@router.get("/chain_intraday/{underlying}")
def get_intraday_chain(
    underlying: str,
    date: date = Query(...),
    time: str = Query(..., description="HHMM format"),
    type: Optional[str] = Query(None),
    expiration: Optional[date] = Query(None),
    strike_min: Optional[float] = Query(None),
    strike_max: Optional[float] = Query(None),
    limit: int = Query(500, ge=1, le=5000),
):
    """Retrieve a stored intraday options snapshot."""
    try:
        df = load_intraday_snapshot(underlying, date, time, expiration, type, strike_min, strike_max)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No intraday snapshot for {underlying} {date} {time}")
    records = df.head(limit).to_dict(orient="records")
    return {"underlying": underlying.upper(), "date": str(date), "time": time, "count": len(records), "contracts": records}


# ── POST /options/backtest/0dte_credit_spread ────────────────────────────────

class ZeroDTERequest(BaseModel):
    underlying: str = Field("SPY", examples=["SPY"])
    date: date = Field(..., examples=["2026-02-06"])
    interval_minutes: int = Field(5, ge=1)
    entry_start: str = Field("1000")
    entry_end: str = Field("1430")
    force_exit: str = Field("1545")
    target_delta_short: float = Field(0.20, gt=0, le=0.50)
    width: float = Field(5, gt=0)
    contracts: int = Field(1, ge=1, le=20)
    profit_take_pct: float = Field(0.50, gt=0)
    stop_loss_pct: float = Field(2.0, gt=0)
    spread_type: str = Field("bull_put")


@router.post("/backtest/0dte_credit_spread")
def backtest_0dte(body: ZeroDTERequest):
    """Run 0DTE credit spread backtest on stored intraday snapshots."""
    try:
        cfg = ZeroDTEConfig(
            underlying=body.underlying, snap_date=body.date,
            interval_minutes=body.interval_minutes,
            entry_start=body.entry_start, entry_end=body.entry_end,
            force_exit=body.force_exit, target_delta_short=body.target_delta_short,
            width=body.width, contracts=body.contracts,
            profit_take_pct=body.profit_take_pct, stop_loss_pct=body.stop_loss_pct,
            spread_type=body.spread_type,
        )
        return run_0dte_backtest(cfg)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("0DTE backtest failed")
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /options/backtest/{run_id}/curve ─────────────────────────────────────

@router.get("/backtest/{run_id}/curve")
def get_backtest_curve(run_id: str):
    """Return PnL curve for a backtest run."""
    from pathlib import Path
    settings = get_settings()
    curve_path = Path(settings.data_dir) / "options" / "pnl_curves" / f"run_id={run_id}" / "curve.parquet"
    if not curve_path.exists():
        raise HTTPException(status_code=404, detail=f"No PnL curve for run {run_id}")
    import pandas as pd
    df = pd.read_parquet(curve_path)
    return {"run_id": run_id, "curve": df.to_dict(orient="records")}
