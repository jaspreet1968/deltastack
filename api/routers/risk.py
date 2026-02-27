"""POST /risk/evaluate_plan – evaluate proposed orders against risk limits."""

from __future__ import annotations

import logging
from typing import List

from fastapi import APIRouter
from pydantic import BaseModel, Field

from deltastack.risk.engine import evaluate_plan

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/risk", tags=["risk"])


class OrderProposal(BaseModel):
    ticker: str
    side: str = "BUY"
    qty: float = 1


class RiskEvaluateRequest(BaseModel):
    orders: List[OrderProposal]


@router.post("/evaluate_plan")
def risk_evaluate(body: RiskEvaluateRequest):
    """Evaluate proposed orders against portfolio risk limits."""
    orders = [o.model_dump() for o in body.orders]
    return evaluate_plan(orders)
