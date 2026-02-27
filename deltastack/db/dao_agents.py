"""DAO for agent tables."""

from __future__ import annotations

import json
import uuid
import logging
from datetime import datetime, timezone
from typing import List, Optional

from deltastack.db.connection import get_db

logger = logging.getLogger(__name__)


def _uid() -> str:
    return uuid.uuid4().hex[:16]


# ── agents ───────────────────────────────────────────────────────────────────

def create_agent(*, name: str, display_name: str = "", description: str = "",
                 risk_profile: str = "BALANCED", broker_provider: str = "paper") -> str:
    c = get_db()
    agent_id = _uid()
    c.execute(
        """INSERT INTO agents (agent_id, name, display_name, description, risk_profile, broker_provider)
           VALUES (?,?,?,?,?,?)""",
        [agent_id, name, display_name, description, risk_profile, broker_provider],
    )
    return agent_id


def get_agent(agent_id: str) -> Optional[dict]:
    c = get_db()
    rows = c.execute("SELECT * FROM agents WHERE agent_id = ?", [agent_id]).fetchall()
    if not rows:
        return None
    cols = [d[0] for d in c.description]
    return dict(zip(cols, rows[0]))


def get_agent_by_name(name: str) -> Optional[dict]:
    c = get_db()
    rows = c.execute("SELECT * FROM agents WHERE name = ?", [name]).fetchall()
    if not rows:
        return None
    cols = [d[0] for d in c.description]
    return dict(zip(cols, rows[0]))


def list_agents() -> List[dict]:
    c = get_db()
    rows = c.execute("SELECT * FROM agents ORDER BY created_at DESC").fetchall()
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in rows]


def update_agent(agent_id: str, **kwargs) -> None:
    c = get_db()
    allowed = {"display_name", "description", "risk_profile", "broker_provider", "enabled"}
    sets = []
    vals = []
    for k, v in kwargs.items():
        if k in allowed:
            sets.append(f"{k} = ?")
            vals.append(v)
    if not sets:
        return
    sets.append("updated_at = current_timestamp")
    vals.append(agent_id)
    c.execute(f"UPDATE agents SET {', '.join(sets)} WHERE agent_id = ?", vals)


# ── agent_strategies ─────────────────────────────────────────────────────────

def add_agent_strategy(*, agent_id: str, strategy_name: str, params: dict = None,
                        schedule: dict = None, execution_mode: str = "plan_only",
                        enabled: bool = True) -> str:
    c = get_db()
    sid = _uid()
    c.execute(
        """INSERT INTO agent_strategies
           (agent_strategy_id, agent_id, strategy_name, params_json, schedule_json, execution_mode, enabled)
           VALUES (?,?,?,?,?,?,?)""",
        [sid, agent_id, strategy_name, json.dumps(params or {}),
         json.dumps(schedule or {}), execution_mode, enabled],
    )
    return sid


def get_agent_strategies(agent_id: str) -> List[dict]:
    c = get_db()
    rows = c.execute(
        "SELECT * FROM agent_strategies WHERE agent_id = ? ORDER BY created_at", [agent_id]
    ).fetchall()
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in rows]


def update_agent_strategy(agent_strategy_id: str, **kwargs) -> None:
    c = get_db()
    allowed = {"params_json", "schedule_json", "execution_mode", "enabled"}
    sets = []
    vals = []
    for k, v in kwargs.items():
        if k in allowed:
            sets.append(f"{k} = ?")
            vals.append(v if not isinstance(v, dict) else json.dumps(v))
    if not sets:
        return
    sets.append("updated_at = current_timestamp")
    vals.append(agent_strategy_id)
    c.execute(f"UPDATE agent_strategies SET {', '.join(sets)} WHERE agent_strategy_id = ?", vals)


# ── agent_runs ───────────────────────────────────────────────────────────────

def insert_agent_run(*, agent_id: str, agent_strategy_id: str = "",
                      run_type: str = "signal", status: str = "running",
                      summary: dict = None) -> str:
    c = get_db()
    run_id = _uid()
    c.execute(
        """INSERT INTO agent_runs (run_id, agent_id, agent_strategy_id, run_type, status, summary_json)
           VALUES (?,?,?,?,?,?)""",
        [run_id, agent_id, agent_strategy_id, run_type, status, json.dumps(summary or {})],
    )
    return run_id


def complete_agent_run(run_id: str, status: str = "success", summary: dict = None) -> None:
    c = get_db()
    c.execute(
        "UPDATE agent_runs SET status=?, ended_at=current_timestamp, summary_json=? WHERE run_id=?",
        [status, json.dumps(summary or {}), run_id],
    )


def get_agent_runs(agent_id: str, limit: int = 20) -> List[dict]:
    c = get_db()
    rows = c.execute(
        "SELECT * FROM agent_runs WHERE agent_id = ? ORDER BY started_at DESC LIMIT ?",
        [agent_id, limit],
    ).fetchall()
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in rows]


# ── run_agent_map ────────────────────────────────────────────────────────────

def map_run_to_agent(run_id: str, agent_id: str, agent_strategy_id: str = "") -> None:
    c = get_db()
    c.execute(
        "INSERT INTO run_agent_map (run_id, agent_id, agent_strategy_id) VALUES (?,?,?)",
        [run_id, agent_id, agent_strategy_id],
    )


# ── seed ─────────────────────────────────────────────────────────────────────

def seed_mad_max() -> str:
    """Create the default Mad Max agent if it doesn't exist."""
    existing = get_agent_by_name("mad_max")
    if existing:
        return existing["agent_id"]

    agent_id = create_agent(
        name="mad_max",
        display_name="Mad Max",
        description="Aggressive paper trading agent with high risk tolerance",
        risk_profile="SUPER_RISKY",
        broker_provider="paper",
    )

    add_agent_strategy(
        agent_id=agent_id,
        strategy_name="0dte_credit_spread",
        params={
            "underlying": "QQQ", "interval_minutes": 5, "width": 2,
            "target_delta_short": 0.20, "entry_start": "1000", "entry_end": "1415",
            "force_exit": "1545", "contracts": 1,
            "min_volume": 100, "max_bid_ask_pct": 0.20,
            "profit_take_pct": 0.50, "stop_loss_pct": 2.00,
        },
        schedule={"interval_minutes": 5, "days_of_week": "mon,tue,wed,thu,fri",
                  "market_hours_only": True},
        execution_mode="plan_only",
    )

    add_agent_strategy(
        agent_id=agent_id,
        strategy_name="credit_spread",
        params={"dte": 21, "spread_width": 5, "target_delta_short": 0.30, "contracts": 2},
        schedule={"interval_minutes": 60, "days_of_week": "mon,tue,wed,thu,fri"},
        execution_mode="plan_only",
        enabled=False,
    )

    add_agent_strategy(
        agent_id=agent_id,
        strategy_name="portfolio_sma",
        params={"fast": 5, "slow": 20, "max_positions": 5, "risk_per_trade": 0.05},
        schedule={"interval_minutes": 30, "days_of_week": "mon,tue,wed,thu,fri"},
        execution_mode="plan_only",
        enabled=False,
    )

    logger.info("Seeded Mad Max agent: %s", agent_id)
    return agent_id
