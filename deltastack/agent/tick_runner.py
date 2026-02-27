"""Tick runner for 0DTE agents – evaluates one snapshot time.

Deterministic: given agent + date + time, always produces the same decision.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import date
from typing import Optional

import pandas as pd

from deltastack.config import get_settings
from deltastack.db.dao_agents import (
    get_agent_by_name, get_agent_strategies,
    insert_agent_run, complete_agent_run,
)
from deltastack.ingest.options_intraday import load_intraday_snapshot, list_available_times

logger = logging.getLogger(__name__)


def run_tick(
    agent_name: str,
    tick_date: date,
    tick_time: str,
    mode: str = "plan_only",
) -> dict:
    """Evaluate one tick for a 0DTE agent."""
    agent = get_agent_by_name(agent_name)
    if not agent:
        raise ValueError(f"Agent '{agent_name}' not found")

    agent_id = agent["agent_id"]
    strategies = get_agent_strategies(agent_id)
    dte_strats = [
        s for s in strategies
        if s["strategy_name"] == "0dte_credit_spread" and s.get("enabled", True)
    ]

    if not dte_strats:
        return {"agent": agent_name, "tick": tick_time, "status": "no_0dte_strategy"}

    strat = dte_strats[0]
    params = json.loads(strat["params_json"]) if isinstance(strat["params_json"], str) else strat["params_json"]
    underlying = params.get("underlying", "QQQ")

    run_id = insert_agent_run(
        agent_id=agent_id,
        agent_strategy_id=strat["agent_strategy_id"],
        run_type="tick",
        status="running",
    )

    # Find nearest snapshot <= tick_time
    available = list_available_times(underlying, tick_date)
    available_times = sorted([t["time"] for t in available])
    nearest = None
    for t in reversed(available_times):
        if t <= tick_time:
            nearest = t
            break

    if not nearest:
        summary = {"tick_time": tick_time, "decision": "skip", "reason": "no_snapshot_available"}
        complete_agent_run(run_id, "skipped", summary)
        return {"agent": agent_name, "run_id": run_id, **summary}

    # Load snapshot
    try:
        chain = load_intraday_snapshot(underlying, tick_date, nearest)
    except FileNotFoundError:
        summary = {"tick_time": tick_time, "snapshot_time": nearest, "decision": "skip", "reason": "snapshot_load_failed"}
        complete_agent_run(run_id, "failed", summary)
        return {"agent": agent_name, "run_id": run_id, **summary}

    # Check entry window
    entry_start = params.get("entry_start", "1000")
    entry_end = params.get("entry_end", "1415")
    if not (entry_start <= tick_time <= entry_end):
        summary = {"tick_time": tick_time, "decision": "skip", "reason": f"outside_entry_window_{entry_start}_{entry_end}"}
        complete_agent_run(run_id, "skipped", summary)
        return {"agent": agent_name, "run_id": run_id, **summary}

    # Filter to 0DTE puts
    opt_type = "put"
    if "type" in chain.columns:
        chain = chain[chain["type"] == opt_type].copy()
    chain["expiration_dt"] = pd.to_datetime(chain.get("expiration", ""), errors="coerce")
    chain = chain[chain["expiration_dt"].dt.date == tick_date]

    if chain.empty:
        summary = {"tick_time": tick_time, "snapshot_time": nearest, "decision": "skip", "reason": "no_0dte_contracts"}
        complete_agent_run(run_id, "skipped", summary)
        return {"agent": agent_name, "run_id": run_id, **summary}

    # Compute mid
    if "bid" in chain.columns and "ask" in chain.columns:
        chain["mid"] = (pd.to_numeric(chain["bid"], errors="coerce").fillna(0) +
                        pd.to_numeric(chain["ask"], errors="coerce").fillna(0)) / 2
    else:
        chain["mid"] = pd.to_numeric(chain.get("last", 0), errors="coerce").fillna(0)

    chain = chain[chain["mid"] > 0]
    chain["strike_f"] = pd.to_numeric(chain.get("strike"), errors="coerce")
    chain = chain.dropna(subset=["strike_f"])

    # Liquidity filters
    min_vol = params.get("min_volume", 100)
    max_ba = params.get("max_bid_ask_pct", 0.20)
    if "volume" in chain.columns:
        chain = chain[pd.to_numeric(chain["volume"], errors="coerce").fillna(0) >= min_vol]
    if "bid" in chain.columns and "ask" in chain.columns:
        chain["ba_pct"] = (chain["ask_f"] - chain["bid_f"]) / chain["mid"] if "ask_f" in chain.columns else 0
        # Recompute if needed
        if "ba_pct" not in chain.columns or chain["ba_pct"].isna().all():
            b = pd.to_numeric(chain.get("bid", 0), errors="coerce").fillna(0)
            a = pd.to_numeric(chain.get("ask", 0), errors="coerce").fillna(0)
            chain["ba_pct"] = (a - b) / chain["mid"]
        chain = chain[chain["ba_pct"] <= max_ba]

    if chain.empty:
        summary = {"tick_time": tick_time, "snapshot_time": nearest, "decision": "skip", "reason": "no_contracts_pass_filters"}
        complete_agent_run(run_id, "skipped", summary)
        return {"agent": agent_name, "run_id": run_id, **summary}

    # Select short leg by delta
    target_delta = params.get("target_delta_short", 0.20)
    width = params.get("width", 2)

    if "delta" in chain.columns and chain["delta"].notna().any():
        chain["delta_abs"] = pd.to_numeric(chain["delta"], errors="coerce").abs()
        short_leg = chain.iloc[(chain["delta_abs"] - target_delta).abs().argsort().iloc[0]]
    else:
        chain_sorted = chain.sort_values("strike_f")
        idx = max(0, int(len(chain_sorted) * target_delta))
        short_leg = chain_sorted.iloc[min(idx, len(chain_sorted) - 1)]

    short_strike = float(short_leg["strike_f"])
    short_mid = float(short_leg["mid"])
    long_strike = short_strike - width

    long_candidates = chain[(chain["strike_f"] - long_strike).abs() <= 1.0]
    if long_candidates.empty:
        summary = {"tick_time": tick_time, "snapshot_time": nearest, "decision": "skip",
                    "reason": "no_long_leg", "short_strike": short_strike}
        complete_agent_run(run_id, "skipped", summary)
        return {"agent": agent_name, "run_id": run_id, **summary}

    long_leg = long_candidates.iloc[(long_candidates["strike_f"] - long_strike).abs().argsort().iloc[0]]
    long_mid = float(long_leg["mid"])
    credit = short_mid - long_mid

    if credit <= 0:
        summary = {"tick_time": tick_time, "decision": "skip", "reason": "no_credit",
                    "short_strike": short_strike, "long_strike": float(long_leg["strike_f"])}
        complete_agent_run(run_id, "skipped", summary)
        return {"agent": agent_name, "run_id": run_id, **summary}

    max_loss = abs(short_strike - float(long_leg["strike_f"])) - credit

    summary = {
        "tick_time": tick_time,
        "snapshot_time": nearest,
        "decision": "BUY",
        "signal": "OPEN_SPREAD",
        "underlying": underlying,
        "short_strike": short_strike,
        "long_strike": float(long_leg["strike_f"]),
        "credit": round(credit, 4),
        "max_loss": round(max_loss, 4),
        "short_mid": round(short_mid, 4),
        "long_mid": round(long_mid, 4),
        "filters": {"min_volume": min_vol, "max_bid_ask_pct": max_ba},
    }

    complete_agent_run(run_id, "success", summary)
    return {"agent": agent_name, "run_id": run_id, "mode": mode, **summary}
