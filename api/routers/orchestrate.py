"""POST /orchestrate/daily – run full daily cycle."""

from __future__ import annotations

import logging
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from deltastack.config import get_settings
from deltastack.orchestrator.run_daily import run_daily_orchestration

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/orchestrate", tags=["orchestrate"])


class StrategyConfig(BaseModel):
    name: str = Field(..., examples=["sma"])
    params: dict = Field(default_factory=dict)


class OrchestrateRequest(BaseModel):
    date: date = Field(..., examples=["2026-02-06"])
    universe_source: str = Field("file", examples=["file"])
    strategies: List[StrategyConfig] = Field(
        default_factory=lambda: [StrategyConfig(name="sma", params={"fast": 10, "slow": 30})]
    )
    mode: str = Field("dry_run", examples=["dry_run"])
    auto_confirm: bool = False


@router.post("/daily")
def orchestrate_daily(body: OrchestrateRequest):
    """Run the full daily orchestration cycle."""
    settings = get_settings()

    # Safety gate for auto_confirm
    mode = body.mode
    if body.auto_confirm:
        mode = "auto_confirm"
    if mode == "auto_confirm" and not settings.orchestration_auto_confirm_allowed:
        raise HTTPException(
            status_code=403,
            detail="auto_confirm is not allowed. Set ORCHESTRATION_AUTO_CONFIRM_ALLOWED=true in .env.",
        )

    logger.info("Orchestrate daily: date=%s mode=%s strategies=%d", body.date, mode, len(body.strategies))

    try:
        result = run_daily_orchestration(
            run_date=body.date,
            strategies=[s.model_dump() for s in body.strategies],
            mode=mode,
            universe_source=body.universe_source,
        )
        return result
    except Exception as exc:
        logger.exception("Orchestration failed")
        raise HTTPException(status_code=500, detail=str(exc))
