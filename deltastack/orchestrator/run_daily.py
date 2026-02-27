"""Daily orchestration: ingest → signals → plans → (optional confirm).

Modes:
- dry_run: compute signals only, no plans
- plan_only: create execution plans but do not confirm
- auto_confirm: confirm plans (requires ORCHESTRATION_AUTO_CONFIRM_ALLOWED + TRADING_ENABLED)
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import date
from pathlib import Path
from typing import List

from deltastack.config import get_settings
from deltastack.db.connection import get_db
from deltastack.db.dao import insert_signal
from deltastack.orchestrator.registry import get_strategy, list_strategies

logger = logging.getLogger(__name__)


def run_daily_orchestration(
    run_date: date,
    strategies: List[dict],
    mode: str = "dry_run",
    universe_source: str = "file",
) -> dict:
    """Execute the full daily cycle."""
    settings = get_settings()
    batch_id = uuid.uuid4().hex[:16]
    db = get_db()

    logger.info("Orchestration batch_id=%s date=%s mode=%s strategies=%d",
                batch_id, run_date, mode, len(strategies))

    # Record run
    db.execute(
        "INSERT INTO orchestration_runs (batch_id, run_date, mode, status) VALUES (?,?,?,?)",
        [batch_id, str(run_date), mode, "running"],
    )

    # ── 1. Load universe ─────────────────────────────────────────────────
    universe_path = Path(settings.universe_file)
    if not universe_path.exists():
        _complete_run(db, batch_id, "error", {"error": "Universe file not found"})
        return {"batch_id": batch_id, "status": "error", "error": "Universe file not found"}

    tickers = [
        line.strip().upper()
        for line in universe_path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    # ── 2. Generate signals per strategy ─────────────────────────────────
    all_signals = []
    for strat_cfg in strategies:
        name = strat_cfg.get("name", "sma")
        params = strat_cfg.get("params", {})
        try:
            strat = get_strategy(name)
        except KeyError as exc:
            all_signals.append({"strategy": name, "error": str(exc)})
            continue

        strat_signals = []
        for ticker in tickers:
            try:
                sig = strat["signal_fn"](ticker, params)
                if sig.get("signal"):
                    insert_signal(
                        strategy=name,
                        ticker=ticker,
                        signal=sig["signal"],
                        as_of=sig.get("as_of", str(run_date)),
                        meta={"batch_id": batch_id, **params},
                    )
                strat_signals.append(sig)
            except Exception as exc:
                strat_signals.append({"ticker": ticker, "error": str(exc)})

        all_signals.append({"strategy": name, "signals": strat_signals})

    if mode == "dry_run":
        summary = {
            "tickers": len(tickers),
            "signals": all_signals,
            "plans_created": 0,
        }
        _complete_run(db, batch_id, "completed", summary)
        return {"batch_id": batch_id, "status": "completed", "mode": mode, **summary}

    # ── 3. Build execution plans for BUY signals ─────────────────────────
    plans_created = []
    buy_signals = []
    for strat_result in all_signals:
        for sig in strat_result.get("signals", []):
            if sig.get("signal") == "BUY":
                buy_signals.append(sig)

    if buy_signals and mode in ("plan_only", "auto_confirm"):
        from deltastack.db.dao_options import insert_execution_plan, insert_execution_event
        for sig in buy_signals[:settings.signals_batch_size]:
            plan_id = uuid.uuid4().hex[:12]
            order = {
                "ticker": sig["ticker"],
                "side": "BUY",
                "qty": 1,  # placeholder; risk engine should size
                "order_type": "MARKET",
            }
            insert_execution_plan(
                plan_id=plan_id,
                request_json=json.dumps({"source": "orchestrator", "batch_id": batch_id}),
                orders_json=json.dumps([order]),
                risk_summary=json.dumps({"auto": True}),
                status="pending",
            )
            plans_created.append({"plan_id": plan_id, "ticker": sig["ticker"]})

    # ── 4. Auto-confirm if allowed ───────────────────────────────────────
    confirmations = []
    if mode == "auto_confirm":
        if not settings.orchestration_auto_confirm_allowed:
            _complete_run(db, batch_id, "blocked", {
                "error": "auto_confirm not allowed (ORCHESTRATION_AUTO_CONFIRM_ALLOWED=false)"
            })
            return {
                "batch_id": batch_id,
                "status": "blocked",
                "error": "ORCHESTRATION_AUTO_CONFIRM_ALLOWED is false",
                "plans_created": len(plans_created),
            }
        if not settings.trading_enabled:
            _complete_run(db, batch_id, "blocked", {"error": "TRADING_ENABLED is false"})
            return {
                "batch_id": batch_id,
                "status": "blocked",
                "error": "TRADING_ENABLED is false",
                "plans_created": len(plans_created),
            }
        # Would confirm plans here – but keeping safe for now
        confirmations = [{"plan_id": p["plan_id"], "confirmed": False, "note": "auto_confirm not yet implemented"} for p in plans_created]

    summary = {
        "tickers": len(tickers),
        "signals": all_signals,
        "plans_created": len(plans_created),
        "plans": plans_created,
        "confirmations": confirmations,
    }
    _complete_run(db, batch_id, "completed", summary)
    return {"batch_id": batch_id, "status": "completed", "mode": mode, **summary}


def _complete_run(db, batch_id: str, status: str, summary: dict) -> None:
    db.execute(
        "UPDATE orchestration_runs SET status=?, summary_json=? WHERE batch_id=?",
        [status, json.dumps(summary, default=str), batch_id],
    )
