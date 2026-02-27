"""Agent CRUD, dashboard, and execution endpoints."""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import List, Optional
from dataclasses import asdict

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from deltastack.config import get_settings
from deltastack.db.dao_agents import (
    create_agent, get_agent, list_agents, update_agent,
    add_agent_strategy, get_agent_strategies, update_agent_strategy,
    get_agent_runs, seed_mad_max,
)
from deltastack.db.connection import get_db
from deltastack.agent.runner import run_agent

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/agents", tags=["agents"])


# ── models ───────────────────────────────────────────────────────────────────

class CreateAgentRequest(BaseModel):
    name: str = Field(..., examples=["my_agent"])
    display_name: str = Field("", examples=["My Agent"])
    description: str = ""
    risk_profile: str = Field("BALANCED", examples=["BALANCED"])
    broker_provider: str = Field("paper", examples=["paper"])


class PatchAgentRequest(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    risk_profile: Optional[str] = None
    broker_provider: Optional[str] = None
    enabled: Optional[bool] = None


class AddStrategyRequest(BaseModel):
    strategy_name: str = Field(..., examples=["sma"])
    params: dict = Field(default_factory=dict)
    schedule: dict = Field(default_factory=dict)
    execution_mode: str = Field("plan_only", examples=["plan_only"])


class PatchStrategyRequest(BaseModel):
    params_json: Optional[dict] = None
    schedule_json: Optional[dict] = None
    execution_mode: Optional[str] = None
    enabled: Optional[bool] = None


class RunAgentRequest(BaseModel):
    mode: str = Field("signal", examples=["signal"])
    date: Optional[date] = None


def _serialize(row: dict) -> dict:
    """Serialize timestamps and JSON fields."""
    for k in ("created_at", "updated_at", "started_at", "ended_at"):
        if row.get(k) is not None:
            row[k] = str(row[k])
    for k in ("params_json", "schedule_json", "summary_json", "exposure_json"):
        if isinstance(row.get(k), str):
            try:
                row[k] = json.loads(row[k])
            except (json.JSONDecodeError, TypeError):
                pass
    return row


# ── CRUD ─────────────────────────────────────────────────────────────────────

@router.post("")
def create_agent_endpoint(body: CreateAgentRequest):
    """Create a new agent."""
    try:
        agent_id = create_agent(
            name=body.name,
            display_name=body.display_name,
            description=body.description,
            risk_profile=body.risk_profile,
            broker_provider=body.broker_provider,
        )
        return {"agent_id": agent_id, "name": body.name}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("")
def list_agents_endpoint():
    """List all agents."""
    agents = list_agents()
    return {"agents": [_serialize(a) for a in agents]}


@router.get("/{agent_id}")
def get_agent_endpoint(agent_id: str):
    """Get agent details including strategies."""
    agent = get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")
    strategies = get_agent_strategies(agent_id)
    result = _serialize(agent)
    result["strategies"] = [_serialize(s) for s in strategies]
    return result


@router.patch("/{agent_id}")
def patch_agent_endpoint(agent_id: str, body: PatchAgentRequest):
    """Update agent properties."""
    agent = get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    update_agent(agent_id, **updates)
    return {"agent_id": agent_id, "updated": list(updates.keys())}


# ── strategies ───────────────────────────────────────────────────────────────

@router.post("/{agent_id}/strategies")
def add_strategy_endpoint(agent_id: str, body: AddStrategyRequest):
    """Add a strategy to an agent."""
    agent = get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")
    sid = add_agent_strategy(
        agent_id=agent_id,
        strategy_name=body.strategy_name,
        params=body.params,
        schedule=body.schedule,
        execution_mode=body.execution_mode,
    )
    return {"agent_strategy_id": sid}


@router.patch("/{agent_id}/strategies/{agent_strategy_id}")
def patch_strategy_endpoint(agent_id: str, agent_strategy_id: str, body: PatchStrategyRequest):
    """Update a strategy configuration."""
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    update_agent_strategy(agent_strategy_id, **updates)
    return {"agent_strategy_id": agent_strategy_id, "updated": list(updates.keys())}


# ── run ──────────────────────────────────────────────────────────────────────

@router.post("/{agent_id}/run")
def run_agent_endpoint(agent_id: str, body: RunAgentRequest):
    """Manually trigger an agent run."""
    agent = get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")
    try:
        result = run_agent(
            agent["name"],
            run_date=body.date or date.today(),
            mode=body.mode,
            dry_run=(body.mode == "signal"),
        )
        return result
    except Exception as exc:
        logger.exception("Agent run failed")
        raise HTTPException(status_code=500, detail=str(exc))


# ── dashboard ────────────────────────────────────────────────────────────────

@router.get("/{agent_id}/dashboard")
def agent_dashboard(agent_id: str):
    """Agent-specific KPI dashboard."""
    agent = get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")

    db = get_db()

    # Recent runs
    runs = get_agent_runs(agent_id, limit=10)

    # Latest signals
    sig_rows = db.execute(
        "SELECT * FROM signals WHERE meta_json LIKE ? ORDER BY created_at DESC LIMIT 20",
        [f'%{agent_id}%'],
    ).fetchall()
    sig_cols = [d[0] for d in db.description] if sig_rows else []
    signals = [_serialize(dict(zip(sig_cols, r))) for r in sig_rows]

    # Recent trades (via run_agent_map)
    trade_rows = db.execute(
        """SELECT t.* FROM trades t
           JOIN run_agent_map m ON t.run_id = m.run_id
           WHERE m.agent_id = ?
           ORDER BY t.entry_time DESC LIMIT 20""",
        [agent_id],
    ).fetchall()
    trade_cols = [d[0] for d in db.description] if trade_rows else []
    trades = [_serialize(dict(zip(trade_cols, r))) for r in trade_rows]

    # Recent orders
    order_rows = db.execute(
        """SELECT o.* FROM orders o
           WHERE o.idempotency_key LIKE ?
           ORDER BY o.created_at DESC LIMIT 20""",
        [f'%{agent_id}%'],
    ).fetchall()
    order_cols = [d[0] for d in db.description] if order_rows else []
    orders = [_serialize(dict(zip(order_cols, r))) for r in order_rows]

    # Errors
    error_rows = db.execute(
        "SELECT * FROM errors WHERE context_json LIKE ? ORDER BY created_at DESC LIMIT 10",
        [f'%{agent_id}%'],
    ).fetchall()
    error_cols = [d[0] for d in db.description] if error_rows else []
    errors = [_serialize(dict(zip(error_cols, r))) for r in error_rows]

    return {
        "agent": _serialize(dict(agent)),
        "strategies": [_serialize(s) for s in get_agent_strategies(agent_id)],
        "recent_runs": [_serialize(r) for r in runs],
        "signals": signals,
        "trades": trades,
        "orders": orders,
        "errors": errors,
    }


# ── trades + orders ──────────────────────────────────────────────────────────

@router.get("/{agent_id}/trades")
def agent_trades(agent_id: str, limit: int = Query(50, ge=1, le=500)):
    """Agent trade blotter."""
    db = get_db()
    rows = db.execute(
        """SELECT t.* FROM trades t
           JOIN run_agent_map m ON t.run_id = m.run_id
           WHERE m.agent_id = ?
           ORDER BY t.entry_time DESC LIMIT ?""",
        [agent_id, limit],
    ).fetchall()
    cols = [d[0] for d in db.description] if rows else []
    return {"trades": [_serialize(dict(zip(cols, r))) for r in rows]}


@router.post("/{agent_id}/flatten")
def flatten_agent(agent_id: str):
    """Close all open positions for this agent (paper). Requires TRADING_ENABLED."""
    settings = get_settings()
    if not settings.trading_enabled:
        raise HTTPException(status_code=503, detail="Trading disabled. Set TRADING_ENABLED=true.")

    agent = get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")

    from deltastack.broker.factory import get_broker
    from deltastack.broker.base import OrderRequest
    from deltastack.db.dao_options import insert_execution_event
    broker = get_broker()
    positions = broker.get_positions()
    results = []

    for pos in positions:
        if abs(pos.qty) < 1e-9:
            continue
        side = "SELL" if pos.qty > 0 else "BUY"
        req = OrderRequest(ticker=pos.ticker, side=side, qty=abs(pos.qty))
        result = broker.place_order(req)
        results.append({"ticker": pos.ticker, "side": side, "qty": abs(pos.qty), "status": result.status})

    insert_execution_event(
        plan_id=f"flatten_{agent_id}",
        event_type="flatten",
        details={"agent_id": agent_id, "positions_closed": len(results), "results": results},
    )

    logger.info("Flattened %d positions for agent %s", len(results), agent_id)
    return {"agent_id": agent_id, "positions_closed": len(results), "results": results}


@router.get("/{agent_id}/orders")
def agent_orders(agent_id: str, limit: int = Query(50, ge=1, le=500)):
    """Agent order history."""
    db = get_db()
    rows = db.execute(
        """SELECT * FROM orders
           WHERE idempotency_key LIKE ?
           ORDER BY created_at DESC LIMIT ?""",
        [f'%{agent_id}%', limit],
    ).fetchall()
    cols = [d[0] for d in db.description] if rows else []
    return {"orders": [_serialize(dict(zip(cols, r))) for r in rows]}


# ── tick runner ──────────────────────────────────────────────────────────────

class TickRequest(BaseModel):
    date: date = Field(..., examples=["2026-02-06"])
    time: str = Field(..., examples=["1030"])
    mode: str = Field("plan_only", examples=["plan_only"])


@router.post("/{agent_id}/tick")
def run_tick_endpoint(agent_id: str, body: TickRequest):
    """Run a single tick evaluation for a 0DTE agent."""
    agent = get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")
    from deltastack.agent.tick_runner import run_tick
    try:
        return run_tick(agent["name"], body.date, body.time, body.mode)
    except Exception as exc:
        logger.exception("Tick run failed")
        raise HTTPException(status_code=500, detail=str(exc))


# ── replay ───────────────────────────────────────────────────────────────────

class ReplayRequest(BaseModel):
    date: date = Field(..., examples=["2026-02-06"])
    start_time: str = Field("1000")
    end_time: str = Field("1415")
    interval_minutes: int = Field(5, ge=1)


@router.post("/{agent_id}/replay")
def replay_endpoint(agent_id: str, body: ReplayRequest):
    """Replay agent decisions across a day's snapshots."""
    agent = get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")
    from deltastack.agent.replay import run_replay
    try:
        return run_replay(agent["name"], body.date, body.start_time, body.end_time, body.interval_minutes)
    except Exception as exc:
        logger.exception("Replay failed")
        raise HTTPException(status_code=500, detail=str(exc))


# ── strategy promotion ───────────────────────────────────────────────────────

class PromoteRequest(BaseModel):
    status: str = Field(..., examples=["paper_live"])
    reason: str = Field("", examples=["promoted for testing"])


@router.patch("/{agent_id}/strategies/{agent_strategy_id}/status")
def promote_strategy(agent_id: str, agent_strategy_id: str, body: PromoteRequest):
    """Change strategy lifecycle status: draft -> paper_live -> approved -> disabled."""
    valid = {"draft", "paper_live", "approved", "disabled"}
    if body.status not in valid:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid}")

    db = get_db()
    # Get current status
    rows = db.execute(
        "SELECT execution_mode FROM agent_strategies WHERE agent_strategy_id = ?",
        [agent_strategy_id],
    ).fetchall()
    old_status = rows[0][0] if rows else "unknown"

    # Update
    update_agent_strategy(agent_strategy_id, execution_mode=body.status)

    # Log event
    db.execute(
        "INSERT INTO strategy_status_events (agent_id, agent_strategy_id, old_status, new_status, reason) VALUES (?,?,?,?,?)",
        [agent_id, agent_strategy_id, old_status, body.status, body.reason],
    )

    return {"agent_strategy_id": agent_strategy_id, "old_status": old_status, "new_status": body.status}


@router.get("/{agent_id}/strategies/{agent_strategy_id}/history")
def strategy_history(agent_id: str, agent_strategy_id: str):
    """Return status change history for a strategy."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM strategy_status_events WHERE agent_strategy_id = ? ORDER BY created_at DESC",
        [agent_strategy_id],
    ).fetchall()
    cols = [d[0] for d in db.description] if rows else []
    return {"events": [_serialize(dict(zip(cols, r))) for r in rows]}
