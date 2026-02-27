"""Options credit spread backtest engine.

Supports:
- Bull Put Spread (short put + long put at lower strike)
- Bear Call Spread (short call + long call at higher strike)

Uses stored options snapshots for contract selection and pricing.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional

import pandas as pd

from deltastack.config import get_settings
from deltastack.ingest.options_chain import load_chain
from deltastack.options.greeks import compute_greeks
from deltastack.db.dao_options import insert_options_backtest_run, insert_options_trade

logger = logging.getLogger(__name__)


@dataclass
class CreditSpreadConfig:
    underlying: str
    as_of: date
    spread_type: str = "bull_put"       # bull_put or bear_call
    dte: int = 30                       # target days to expiry
    spread_width: float = 5.0           # strike width
    target_delta_short: float = 0.20
    min_volume: int = 50
    max_bid_ask_pct: float = 0.25       # max bid-ask spread as pct of mid
    contracts: int = 1
    slippage_pct: Optional[float] = None
    exit_profit_take_pct: float = 0.50  # take profit at 50% of credit
    exit_stop_loss_pct: float = 2.00    # stop at 200% of credit (2x)
    exit_dte_close: int = 5             # close at N days before expiry


def run_credit_spread_backtest(cfg: CreditSpreadConfig) -> dict:
    """Run a single credit spread backtest entry using stored options snapshot."""
    run_id = uuid.uuid4().hex[:16]
    settings = get_settings()
    slippage = cfg.slippage_pct if cfg.slippage_pct is not None else settings.options_slippage_pct
    multiplier = settings.options_contract_multiplier
    underlying = cfg.underlying.upper()

    logger.info("Credit spread backtest run_id=%s %s %s as_of=%s", run_id, cfg.spread_type, underlying, cfg.as_of)

    # ── Load snapshot ────────────────────────────────────────────────────
    try:
        chain = load_chain(underlying, cfg.as_of)
    except FileNotFoundError:
        raise ValueError(
            f"No options snapshot for {underlying} as_of={cfg.as_of}. "
            "Ingest first via POST /options/chain/snapshot."
        )

    if chain.empty:
        raise ValueError(f"Options snapshot for {underlying} is empty")

    # ── Determine option type ────────────────────────────────────────────
    if cfg.spread_type == "bull_put":
        opt_type = "put"
    elif cfg.spread_type == "bear_call":
        opt_type = "call"
    else:
        raise ValueError(f"Unsupported spread_type: {cfg.spread_type}")

    # Filter by type
    chain = chain[chain["type"] == opt_type].copy()
    if chain.empty:
        raise ValueError(f"No {opt_type} contracts in snapshot")

    # ── Select expiration closest to target DTE ──────────────────────────
    chain["expiration_dt"] = pd.to_datetime(chain["expiration"], errors="coerce")
    chain = chain.dropna(subset=["expiration_dt"])
    chain["dte_actual"] = (chain["expiration_dt"] - pd.Timestamp(cfg.as_of)).dt.days
    chain = chain[chain["dte_actual"] > 0]

    if chain.empty:
        raise ValueError("No valid expirations found after as_of date")

    target_dte = cfg.dte
    expirations = chain.groupby("expiration")["dte_actual"].first()
    best_exp = expirations.iloc[(expirations - target_dte).abs().argsort().iloc[0]]
    best_exp_str = expirations.index[(expirations - target_dte).abs().argsort().iloc[0]]
    chain = chain[chain["expiration"] == best_exp_str].copy()

    # ── Apply liquidity filters ──────────────────────────────────────────
    if "volume" in chain.columns:
        chain = chain[chain["volume"].fillna(0) >= cfg.min_volume]

    # Compute mid price and bid-ask filter
    if "bid" in chain.columns and "ask" in chain.columns:
        chain["bid_f"] = pd.to_numeric(chain["bid"], errors="coerce").fillna(0)
        chain["ask_f"] = pd.to_numeric(chain["ask"], errors="coerce").fillna(0)
        chain["mid"] = (chain["bid_f"] + chain["ask_f"]) / 2
        chain = chain[chain["mid"] > 0]
        chain["ba_pct"] = (chain["ask_f"] - chain["bid_f"]) / chain["mid"]
        chain = chain[chain["ba_pct"] <= cfg.max_bid_ask_pct]
    else:
        chain["mid"] = pd.to_numeric(chain.get("last", 0), errors="coerce").fillna(0)
        chain = chain[chain["mid"] > 0]

    if chain.empty:
        raise ValueError("No contracts pass liquidity filters")

    # ── Select short leg by delta ────────────────────────────────────────
    chain["strike_f"] = pd.to_numeric(chain["strike"], errors="coerce")
    chain = chain.dropna(subset=["strike_f"])

    if "delta" in chain.columns and chain["delta"].notna().any():
        chain["delta_f"] = pd.to_numeric(chain["delta"], errors="coerce").abs()
        short_leg = chain.iloc[(chain["delta_f"] - cfg.target_delta_short).abs().argsort().iloc[0]]
    else:
        # Fallback: pick strike based on approximate delta from BS
        # For puts, lower delta = further OTM = lower strike
        chain_sorted = chain.sort_values("strike_f", ascending=(opt_type == "put"))
        # Pick ~20th percentile for OTM
        idx = max(0, int(len(chain_sorted) * cfg.target_delta_short))
        short_leg = chain_sorted.iloc[min(idx, len(chain_sorted) - 1)]

    short_strike = float(short_leg["strike_f"])
    short_mid = float(short_leg["mid"])

    # ── Select long leg by width ─────────────────────────────────────────
    if opt_type == "put":
        long_strike = short_strike - cfg.spread_width
    else:
        long_strike = short_strike + cfg.spread_width

    long_candidates = chain[
        (chain["strike_f"] - long_strike).abs() <= 1.0  # tolerance
    ]
    if long_candidates.empty:
        # Find nearest available
        long_candidates = chain.copy()
        if opt_type == "put":
            long_candidates = long_candidates[long_candidates["strike_f"] < short_strike]
        else:
            long_candidates = long_candidates[long_candidates["strike_f"] > short_strike]

    if long_candidates.empty:
        raise ValueError(f"Cannot find long leg for width={cfg.spread_width}")

    long_leg = long_candidates.iloc[
        (long_candidates["strike_f"] - long_strike).abs().argsort().iloc[0]
    ]
    long_strike = float(long_leg["strike_f"])
    long_mid = float(long_leg["mid"])

    # ── Calculate credit and max loss ────────────────────────────────────
    credit_per_share = short_mid - long_mid
    if credit_per_share <= 0:
        raise ValueError(
            f"No credit: short_mid={short_mid:.2f} <= long_mid={long_mid:.2f}. "
            "Try different parameters."
        )

    # Apply slippage (reduces credit received)
    credit_per_share *= (1 - slippage)
    width = abs(short_strike - long_strike)
    max_loss_per_share = width - credit_per_share

    total_credit = credit_per_share * multiplier * cfg.contracts
    total_max_loss = max_loss_per_share * multiplier * cfg.contracts

    # ── Expiry outcome (simplified – assumes held to expiry) ─────────────
    # We don't have later snapshots for mark-to-market in this version,
    # so we compute at-expiry P&L based on the spread structure.
    # The actual P&L depends on underlying price at expiry which we don't know,
    # so we report the credit and max loss as the range.
    metrics = {
        "spread_type": cfg.spread_type,
        "underlying": underlying,
        "as_of": str(cfg.as_of),
        "expiration": best_exp_str,
        "dte_actual": int(best_exp),
        "short_strike": short_strike,
        "long_strike": long_strike,
        "short_mid": round(short_mid, 4),
        "long_mid": round(long_mid, 4),
        "credit_per_share": round(credit_per_share, 4),
        "max_loss_per_share": round(max_loss_per_share, 4),
        "total_credit": round(total_credit, 2),
        "total_max_loss": round(total_max_loss, 2),
        "contracts": cfg.contracts,
        "multiplier": multiplier,
        "risk_reward_ratio": round(total_max_loss / total_credit, 2) if total_credit > 0 else 0,
        "max_profit": round(total_credit, 2),
        "breakeven": round(
            short_strike - credit_per_share if opt_type == "put"
            else short_strike + credit_per_share, 2
        ),
        "exit_rules": {
            "profit_take_pct": cfg.exit_profit_take_pct,
            "stop_loss_pct": cfg.exit_stop_loss_pct,
            "dte_close": cfg.exit_dte_close,
        },
    }

    # ── Persist to DB ────────────────────────────────────────────────────
    try:
        insert_options_backtest_run(
            run_id=run_id,
            strategy=cfg.spread_type,
            underlying=underlying,
            params={
                "dte": cfg.dte,
                "spread_width": cfg.spread_width,
                "target_delta_short": cfg.target_delta_short,
                "contracts": cfg.contracts,
                "slippage_pct": slippage,
            },
            metrics=metrics,
        )
        insert_options_trade(
            run_id=run_id,
            underlying=underlying,
            strategy=cfg.spread_type,
            short_strike=short_strike,
            long_strike=long_strike,
            expiration=best_exp_str,
            option_type=opt_type,
            contracts=cfg.contracts,
            credit=total_credit,
            max_loss=total_max_loss,
            pnl=0,  # unknown until expiry
            exit_reason="open",
        )
    except Exception:
        logger.exception("Failed to persist options backtest run %s", run_id)

    logger.info(
        "Credit spread %s %s: short=%.0f long=%.0f credit=%.2f max_loss=%.2f",
        cfg.spread_type, underlying, short_strike, long_strike, total_credit, total_max_loss,
    )

    return {
        "run_id": run_id,
        **metrics,
    }
