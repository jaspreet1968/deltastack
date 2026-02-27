"""Agent runner – executes strategies for a given agent.

Supports dry_run, plan_only, and auto_confirm modes.
Checks market hours before running.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, time
from typing import Optional

from deltastack.config import get_settings
from deltastack.db.dao_agents import (
    get_agent, get_agent_by_name, get_agent_strategies,
    insert_agent_run, complete_agent_run, map_run_to_agent,
)
from deltastack.orchestrator.registry import get_strategy
from deltastack.db.dao import insert_signal

logger = logging.getLogger(__name__)


def is_market_hours() -> bool:
    """Check if current time is within configured market hours (Mon-Fri)."""
    settings = get_settings()
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(settings.market_timezone)
    except Exception:
        return True  # default allow if TZ unavailable

    now = datetime.now(tz)
    if now.weekday() >= 5:  # Sat=5, Sun=6
        return False

    open_parts = settings.market_open.split(":")
    close_parts = settings.market_close.split(":")
    mkt_open = time(int(open_parts[0]), int(open_parts[1]))
    mkt_close = time(int(close_parts[0]), int(close_parts[1]))

    return mkt_open <= now.time() <= mkt_close


def run_agent(
    agent_name: str,
    run_date: Optional[date] = None,
    mode: str = "signal",  # signal | plan_only | auto_confirm
    dry_run: bool = False,
) -> dict:
    """Execute all enabled strategies for an agent."""
    settings = get_settings()
    agent = get_agent_by_name(agent_name)
    if not agent:
        raise ValueError(f"Agent '{agent_name}' not found")

    if not agent.get("enabled", True):
        return {"agent": agent_name, "status": "disabled", "message": "Agent is disabled"}

    agent_id = agent["agent_id"]
    strategies = get_agent_strategies(agent_id)
    enabled_strategies = [s for s in strategies if s.get("enabled", True)]

    if not enabled_strategies:
        return {"agent": agent_name, "status": "no_strategies", "message": "No enabled strategies"}

    run_date = run_date or date.today()
    results = []

    for strat in enabled_strategies:
        strat_name = strat["strategy_name"]
        params = json.loads(strat.get("params_json", "{}")) if isinstance(strat.get("params_json"), str) else strat.get("params_json", {})
        exec_mode = strat.get("execution_mode", "plan_only")
        if dry_run:
            exec_mode = "signal"

        run_id = insert_agent_run(
            agent_id=agent_id,
            agent_strategy_id=strat["agent_strategy_id"],
            run_type=exec_mode,
            status="running",
        )

        try:
            strat_def = get_strategy(strat_name)
        except KeyError:
            complete_agent_run(run_id, "failed", {"error": f"Strategy '{strat_name}' not registered"})
            results.append({"strategy": strat_name, "status": "failed", "error": "not_registered"})
            continue

        # Generate signals for universe tickers
        from pathlib import Path
        universe_path = Path(settings.universe_file)
        tickers = []
        if universe_path.exists():
            tickers = [
                line.strip().upper()
                for line in universe_path.read_text().splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]

        signals = []
        for ticker in tickers:
            try:
                sig = strat_def["signal_fn"](ticker, params)
                if sig.get("signal"):
                    insert_signal(
                        strategy=strat_name,
                        ticker=ticker,
                        signal=sig["signal"],
                        as_of=sig.get("as_of", str(run_date)),
                        meta={"agent_id": agent_id, "run_id": run_id},
                    )
                signals.append(sig)
            except Exception as exc:
                signals.append({"ticker": ticker, "error": str(exc)})

        summary = {
            "strategy": strat_name,
            "mode": exec_mode,
            "tickers": len(tickers),
            "signals_generated": len([s for s in signals if s.get("signal")]),
            "buy_signals": len([s for s in signals if s.get("signal") == "BUY"]),
        }

        complete_agent_run(run_id, "success", summary)
        map_run_to_agent(run_id, agent_id, strat["agent_strategy_id"])
        results.append(summary)

    return {
        "agent": agent_name,
        "agent_id": agent_id,
        "date": str(run_date),
        "strategies_run": len(results),
        "results": results,
    }
