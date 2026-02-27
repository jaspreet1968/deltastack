"""0DTE credit spread backtest – event-driven intraday simulation.

Steps through intraday option snapshots at configurable intervals,
selects same-day expiry spreads, marks-to-market, and enforces
time-based exits.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import date
from typing import List, Optional

import pandas as pd

from deltastack.config import get_settings
from deltastack.ingest.options_intraday import load_intraday_snapshot, list_available_times
from deltastack.db.dao_options import insert_options_backtest_run, insert_options_trade

logger = logging.getLogger(__name__)


@dataclass
class ZeroDTEConfig:
    underlying: str = "SPY"
    snap_date: date = date.today()
    interval_minutes: int = 5
    entry_start: str = "1000"
    entry_end: str = "1430"
    force_exit: str = "1545"
    target_delta_short: float = 0.20
    width: float = 5.0
    contracts: int = 1
    profit_take_pct: float = 0.50
    stop_loss_pct: float = 2.00
    spread_type: str = "bull_put"


def run_0dte_backtest(cfg: ZeroDTEConfig) -> dict:
    """Run 0DTE credit spread backtest on stored intraday snapshots."""
    run_id = uuid.uuid4().hex[:16]
    settings = get_settings()
    underlying = cfg.underlying.upper()
    multiplier = settings.options_contract_multiplier
    slippage = settings.options_slippage_pct

    logger.info("0DTE backtest run_id=%s %s %s", run_id, underlying, cfg.snap_date)

    # Get available times
    times = list_available_times(underlying, cfg.snap_date)
    if not times:
        raise ValueError(f"No intraday snapshots for {underlying} on {cfg.snap_date}")

    available_times = [t["time"] for t in times]
    opt_type = "put" if cfg.spread_type == "bull_put" else "call"

    # Filter to trading window
    entry_times = [t for t in available_times if cfg.entry_start <= t <= cfg.entry_end]
    exit_times = [t for t in available_times if t <= cfg.force_exit]

    trades = []
    pnl_curve = []
    open_position = None

    for snap_time in exit_times:
        # Try entry if no position open and within entry window
        if open_position is None and snap_time in entry_times:
            try:
                chain = load_intraday_snapshot(underlying, cfg.snap_date, snap_time)
                chain = chain[chain["type"] == opt_type].copy() if "type" in chain.columns else chain

                # Filter to 0DTE (same-day expiry)
                chain["expiration_dt"] = pd.to_datetime(chain.get("expiration", ""), errors="coerce")
                chain = chain[chain["expiration_dt"].dt.date == cfg.snap_date]

                if chain.empty:
                    continue

                # Compute mid prices
                if "bid" in chain.columns and "ask" in chain.columns:
                    chain["bid_f"] = pd.to_numeric(chain["bid"], errors="coerce").fillna(0)
                    chain["ask_f"] = pd.to_numeric(chain["ask"], errors="coerce").fillna(0)
                    chain["mid"] = (chain["bid_f"] + chain["ask_f"]) / 2
                else:
                    chain["mid"] = pd.to_numeric(chain.get("last", 0), errors="coerce").fillna(0)

                chain = chain[chain["mid"] > 0]
                chain["strike_f"] = pd.to_numeric(chain["strike"], errors="coerce")
                chain = chain.dropna(subset=["strike_f"])

                if chain.empty:
                    continue

                # Select short leg
                if "delta" in chain.columns and chain["delta"].notna().any():
                    chain["delta_abs"] = pd.to_numeric(chain["delta"], errors="coerce").abs()
                    short_leg = chain.iloc[(chain["delta_abs"] - cfg.target_delta_short).abs().argsort().iloc[0]]
                else:
                    chain_sorted = chain.sort_values("strike_f")
                    idx = max(0, int(len(chain_sorted) * cfg.target_delta_short))
                    short_leg = chain_sorted.iloc[min(idx, len(chain_sorted) - 1)]

                short_strike = float(short_leg["strike_f"])
                short_mid = float(short_leg["mid"])

                # Long leg
                if opt_type == "put":
                    long_target = short_strike - cfg.width
                else:
                    long_target = short_strike + cfg.width

                long_candidates = chain[(chain["strike_f"] - long_target).abs() <= 1.0]
                if long_candidates.empty:
                    continue

                long_leg = long_candidates.iloc[(long_candidates["strike_f"] - long_target).abs().argsort().iloc[0]]
                long_strike = float(long_leg["strike_f"])
                long_mid = float(long_leg["mid"])

                credit = (short_mid - long_mid) * (1 - slippage)
                if credit <= 0:
                    continue

                max_loss = abs(short_strike - long_strike) - credit
                total_credit = credit * multiplier * cfg.contracts

                open_position = {
                    "entry_time": snap_time,
                    "short_strike": short_strike,
                    "long_strike": long_strike,
                    "credit": credit,
                    "max_loss": max_loss,
                    "total_credit": total_credit,
                }
            except (FileNotFoundError, ValueError):
                continue

        # Mark-to-market if position open
        if open_position is not None:
            # Check exits
            exit_reason = None
            current_value = open_position["credit"]  # default no change

            try:
                chain = load_intraday_snapshot(underlying, cfg.snap_date, snap_time)
                chain = chain[chain["type"] == opt_type].copy() if "type" in chain.columns else chain
                chain["strike_f"] = pd.to_numeric(chain.get("strike"), errors="coerce")
                if "bid" in chain.columns and "ask" in chain.columns:
                    chain["mid"] = (pd.to_numeric(chain["bid"], errors="coerce").fillna(0) +
                                    pd.to_numeric(chain["ask"], errors="coerce").fillna(0)) / 2
                else:
                    chain["mid"] = pd.to_numeric(chain.get("last", 0), errors="coerce").fillna(0)

                # Find current spread value
                short_now = chain[chain["strike_f"] == open_position["short_strike"]]
                long_now = chain[chain["strike_f"] == open_position["long_strike"]]
                if not short_now.empty and not long_now.empty:
                    current_value = float(short_now.iloc[0]["mid"]) - float(long_now.iloc[0]["mid"])
            except Exception:
                pass

            pnl = (open_position["credit"] - current_value) * multiplier * cfg.contracts

            pnl_curve.append({"time": snap_time, "pnl": round(pnl, 2)})

            # Check profit target
            if pnl >= open_position["total_credit"] * cfg.profit_take_pct:
                exit_reason = "profit_target"
            # Check stop loss
            elif pnl <= -open_position["total_credit"] * cfg.stop_loss_pct:
                exit_reason = "stop_loss"
            # Check forced exit time
            elif snap_time >= cfg.force_exit:
                exit_reason = "forced_exit"
            # Check max minutes
            elif len(pnl_curve) * cfg.interval_minutes >= settings.max_0dte_position_minutes:
                exit_reason = "time_stop"

            if exit_reason:
                trades.append({
                    "entry_time": open_position["entry_time"],
                    "exit_time": snap_time,
                    "short_strike": open_position["short_strike"],
                    "long_strike": open_position["long_strike"],
                    "credit": round(open_position["credit"], 4),
                    "pnl": round(pnl, 2),
                    "exit_reason": exit_reason,
                    "minutes_held": len(pnl_curve) * cfg.interval_minutes,
                })
                open_position = None

    # Close any remaining position at last available time
    if open_position and pnl_curve:
        trades.append({
            "entry_time": open_position["entry_time"],
            "exit_time": exit_times[-1] if exit_times else cfg.force_exit,
            "short_strike": open_position["short_strike"],
            "long_strike": open_position["long_strike"],
            "credit": round(open_position["credit"], 4),
            "pnl": round(pnl_curve[-1]["pnl"], 2),
            "exit_reason": "end_of_data",
            "minutes_held": len(pnl_curve) * cfg.interval_minutes,
        })

    # Metrics
    total_pnl = sum(t["pnl"] for t in trades)
    wins = [t for t in trades if t["pnl"] > 0]
    mae = min((p["pnl"] for p in pnl_curve), default=0)
    mfe = max((p["pnl"] for p in pnl_curve), default=0)
    avg_hold = sum(t["minutes_held"] for t in trades) / len(trades) if trades else 0

    metrics = {
        "total_pnl": round(total_pnl, 2),
        "num_trades": len(trades),
        "win_rate": round(len(wins) / len(trades), 4) if trades else 0,
        "mae": round(mae, 2),
        "mfe": round(mfe, 2),
        "avg_hold_minutes": round(avg_hold, 1),
    }

    # Persist
    try:
        insert_options_backtest_run(
            run_id=run_id, strategy="0dte_credit_spread",
            underlying=underlying,
            params={
                "date": str(cfg.snap_date), "spread_type": cfg.spread_type,
                "width": cfg.width, "delta": cfg.target_delta_short,
                "interval": cfg.interval_minutes,
            },
            metrics=metrics,
        )
        for t in trades:
            insert_options_trade(
                run_id=run_id, underlying=underlying, strategy="0dte_credit_spread",
                short_strike=t["short_strike"], long_strike=t["long_strike"],
                expiration=str(cfg.snap_date), option_type=opt_type,
                contracts=cfg.contracts, credit=t["credit"],
                max_loss=0, pnl=t["pnl"], exit_reason=t["exit_reason"],
            )
    except Exception:
        logger.exception("Failed to persist 0DTE backtest %s", run_id)

    # Save PnL curve
    if pnl_curve:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            curve_dir = Path(settings.data_dir) / "options" / "pnl_curves" / f"run_id={run_id}"
            curve_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.Table.from_pandas(pd.DataFrame(pnl_curve), preserve_index=False),
                curve_dir / "curve.parquet", compression="snappy",
            )
        except Exception:
            logger.warning("Failed to save PnL curve for %s", run_id)

    return {
        "run_id": run_id,
        "underlying": underlying,
        "date": str(cfg.snap_date),
        "metrics": metrics,
        "trades": trades,
        "pnl_curve_length": len(pnl_curve),
    }


# Need Path import
from pathlib import Path
