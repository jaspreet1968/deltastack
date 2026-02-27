"""Replay mode – step through a day's ticks to debug decisions.

Produces a timeline of what the agent would have done at each snapshot time.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import date
from typing import List

from deltastack.agent.tick_runner import run_tick
from deltastack.db.connection import get_db
from deltastack.db.dao_agents import get_agent_by_name
from deltastack.ingest.options_intraday import list_available_times

logger = logging.getLogger(__name__)


def run_replay(
    agent_name: str,
    replay_date: date,
    start_time: str = "1000",
    end_time: str = "1415",
    interval_minutes: int = 5,
) -> dict:
    """Replay agent decisions across a day's snapshots."""
    agent = get_agent_by_name(agent_name)
    if not agent:
        raise ValueError(f"Agent '{agent_name}' not found")

    agent_id = agent["agent_id"]
    replay_id = uuid.uuid4().hex[:16]

    # Get all strategies to find underlying
    from deltastack.db.dao_agents import get_agent_strategies
    strategies = get_agent_strategies(agent_id)
    dte_strats = [s for s in strategies if s["strategy_name"] == "0dte_credit_spread"]
    if dte_strats:
        params = json.loads(dte_strats[0]["params_json"]) if isinstance(dte_strats[0]["params_json"], str) else dte_strats[0]["params_json"]
        underlying = params.get("underlying", "QQQ")
    else:
        underlying = "QQQ"

    # Get available snapshot times
    available = list_available_times(underlying, replay_date)
    all_times = sorted([t["time"] for t in available])

    # Filter to window
    tick_times = [t for t in all_times if start_time <= t <= end_time]

    # Thin to interval
    if interval_minutes > 1 and tick_times:
        thinned = [tick_times[0]]
        for t in tick_times[1:]:
            # Simple time diff check
            prev = thinned[-1]
            prev_mins = int(prev[:2]) * 60 + int(prev[2:])
            curr_mins = int(t[:2]) * 60 + int(t[2:])
            if curr_mins - prev_mins >= interval_minutes:
                thinned.append(t)
        tick_times = thinned

    # Record replay
    db = get_db()
    db.execute(
        "INSERT INTO agent_replays (replay_id, agent_id, replay_date, params_json) VALUES (?,?,?,?)",
        [replay_id, agent_id, str(replay_date),
         json.dumps({"start_time": start_time, "end_time": end_time, "interval": interval_minutes})],
    )

    # Run each tick
    timeline = []
    for t in tick_times:
        try:
            result = run_tick(agent_name, replay_date, t, mode="plan_only")
            tick_entry = {
                "time": t,
                "decision": result.get("decision", "unknown"),
                "signal": result.get("signal", ""),
                "short_strike": result.get("short_strike"),
                "long_strike": result.get("long_strike"),
                "credit": result.get("credit"),
                "reason": result.get("reason", ""),
            }
        except Exception as exc:
            tick_entry = {"time": t, "decision": "error", "reason": str(exc)}

        timeline.append(tick_entry)

        # Persist tick
        db.execute(
            "INSERT INTO agent_replay_ticks (replay_id, tick_time, signal_json, decision_json) VALUES (?,?,?,?)",
            [replay_id, t, json.dumps({"signal": tick_entry.get("signal", "")}),
             json.dumps(tick_entry)],
        )

    return {
        "replay_id": replay_id,
        "agent": agent_name,
        "date": str(replay_date),
        "underlying": underlying,
        "ticks_evaluated": len(timeline),
        "timeline": timeline,
    }
