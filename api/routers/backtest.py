"""Backtest endpoints – SMA crossover, Buy-and-Hold, and Portfolio SMA."""

from __future__ import annotations

import logging
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from deltastack.backtest.sma import run_sma_backtest
from deltastack.backtest.buy_hold import run_buy_hold_backtest
from deltastack.backtest.portfolio_sma import PortfolioConfig, run_portfolio_sma_backtest
from deltastack.backtest.walk_forward import run_walk_forward_sma

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/backtest", tags=["backtest"])


# ── request models ───────────────────────────────────────────────────────────

class SMABacktestRequest(BaseModel):
    ticker: str = Field(..., examples=["AAPL"])
    start: date = Field(..., examples=["2020-01-01"])
    end: date = Field(..., examples=["2025-12-31"])
    fast: int = Field(10, ge=2, le=200)
    slow: int = Field(30, ge=5, le=500)


class BuyHoldRequest(BaseModel):
    ticker: str = Field(..., examples=["AAPL"])
    start: date = Field(..., examples=["2020-01-01"])
    end: date = Field(..., examples=["2025-12-31"])


class PortfolioSMARequest(BaseModel):
    tickers: List[str] = Field(..., examples=[["AAPL", "MSFT", "NVDA"]])
    start: date = Field(..., examples=["2024-01-01"])
    end: date = Field(..., examples=["2025-12-31"])
    fast: int = Field(10, ge=2, le=200)
    slow: int = Field(30, ge=5, le=500)
    initial_cash: float = Field(100_000, gt=0)
    max_positions: int = Field(3, ge=1, le=50)
    risk_per_trade: float = Field(0.02, gt=0, le=1.0)
    commission_per_trade: float = Field(1.0, ge=0)
    slippage_bps: float = Field(2.0, ge=0)
    include_curve: bool = Field(False, description="Include full equity curve in response")


# ── POST /backtest/sma ───────────────────────────────────────────────────────

@router.post("/sma")
def backtest_sma(body: SMABacktestRequest):
    """Run a simple SMA crossover backtest on stored bars."""
    logger.info(
        "Backtest SMA request: %s [%s – %s] fast=%d slow=%d",
        body.ticker, body.start, body.end, body.fast, body.slow,
    )
    try:
        result = run_sma_backtest(
            ticker=body.ticker,
            start=body.start,
            end=body.end,
            fast=body.fast,
            slow=body.slow,
        )
        return result.to_dict()
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"No price data for {body.ticker.upper()}. Ingest data first via POST /ingest/daily.",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Backtest failed")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /backtest/buy_hold ──────────────────────────────────────────────────

@router.post("/buy_hold")
def backtest_buy_hold(body: BuyHoldRequest):
    """Run a buy-and-hold benchmark backtest on stored bars."""
    logger.info("Backtest buy-hold: %s [%s – %s]", body.ticker, body.start, body.end)
    try:
        result = run_buy_hold_backtest(
            ticker=body.ticker,
            start=body.start,
            end=body.end,
        )
        return result.to_dict()
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"No price data for {body.ticker.upper()}. Ingest data first via POST /ingest/daily.",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Buy-hold backtest failed")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /backtest/portfolio_sma ─────────────────────────────────────────────

@router.post("/portfolio_sma")
def backtest_portfolio_sma(body: PortfolioSMARequest):
    """Run a portfolio-aware SMA crossover backtest across multiple tickers."""
    logger.info(
        "Portfolio SMA backtest: tickers=%s [%s – %s] fast=%d slow=%d cash=%.0f",
        body.tickers, body.start, body.end, body.fast, body.slow, body.initial_cash,
    )
    try:
        cfg = PortfolioConfig(
            tickers=[t.upper() for t in body.tickers],
            start=body.start,
            end=body.end,
            fast=body.fast,
            slow=body.slow,
            initial_cash=body.initial_cash,
            max_positions=body.max_positions,
            risk_per_trade=body.risk_per_trade,
            commission_per_trade=body.commission_per_trade,
            slippage_bps=body.slippage_bps,
        )
        result = run_portfolio_sma_backtest(cfg)

        # Optionally strip equity curve to reduce payload
        if not body.include_curve:
            result.pop("equity_curve", None)

        return result
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Portfolio backtest failed")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /backtest/walk_forward/sma ──────────────────────────────────────────

class WalkForwardRequest(BaseModel):
    ticker: str = Field(..., examples=["AAPL"])
    start: date = Field(..., examples=["2016-01-01"])
    end: date = Field(..., examples=["2026-01-01"])
    train_window_days: int = Field(504, ge=60)
    test_window_days: int = Field(63, ge=10)
    param_grid: dict = Field(
        default_factory=lambda: {"fast": [5, 10, 20], "slow": [30, 50, 100]}
    )


@router.post("/walk_forward/sma")
def backtest_walk_forward_sma(body: WalkForwardRequest):
    """Run walk-forward validation for SMA strategy."""
    logger.info("WFA request: %s [%s – %s] train=%dd test=%dd",
                body.ticker, body.start, body.end, body.train_window_days, body.test_window_days)
    try:
        return run_walk_forward_sma(
            ticker=body.ticker,
            start=body.start,
            end=body.end,
            train_window_days=body.train_window_days,
            test_window_days=body.test_window_days,
            param_grid=body.param_grid,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("WFA failed")
        raise HTTPException(status_code=500, detail=str(exc))
