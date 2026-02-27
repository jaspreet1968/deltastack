"""Strategy registry – maps strategy names to their implementations."""

from __future__ import annotations

import logging
from typing import Callable, Dict, List

logger = logging.getLogger(__name__)

# Registry: name -> callable that generates signals for a ticker
_STRATEGIES: Dict[str, dict] = {}


def register(name: str, *, requires: List[str], signal_fn: Callable) -> None:
    """Register a strategy by name."""
    _STRATEGIES[name] = {
        "requires": requires,
        "signal_fn": signal_fn,
    }


def get_strategy(name: str) -> dict:
    if name not in _STRATEGIES:
        raise KeyError(f"Strategy '{name}' not registered. Available: {list(_STRATEGIES.keys())}")
    return _STRATEGIES[name]


def list_strategies() -> List[str]:
    return list(_STRATEGIES.keys())


# ── auto-register built-in strategies ────────────────────────────────────────

def _sma_signal(ticker: str, params: dict) -> dict:
    """Generate SMA signal for a single ticker."""
    from deltastack.data.storage import load_bars, ticker_exists
    if not ticker_exists(ticker):
        return {"ticker": ticker, "signal": None, "reason": "no_data"}

    fast = params.get("fast", 10)
    slow = params.get("slow", 30)

    df = load_bars(ticker, limit=100_000)
    if len(df) < slow + 1:
        return {"ticker": ticker, "signal": None, "reason": "insufficient_data"}

    df = df.sort_values("date").reset_index(drop=True)
    df["close_f"] = df["close"].astype(float)
    df["sma_fast"] = df["close_f"].rolling(window=fast).mean()
    df["sma_slow"] = df["close_f"].rolling(window=slow).mean()
    df = df.dropna(subset=["sma_fast", "sma_slow"])

    if len(df) < 2:
        return {"ticker": ticker, "signal": None, "reason": "insufficient_sma_data"}

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    fv, sv = float(latest["sma_fast"]), float(latest["sma_slow"])

    if float(prev["sma_fast"]) <= float(prev["sma_slow"]) and fv > sv:
        sig = "BUY"
    elif float(prev["sma_fast"]) >= float(prev["sma_slow"]) and fv < sv:
        sig = "SELL"
    else:
        sig = "HOLD"

    return {
        "ticker": ticker,
        "signal": sig,
        "as_of": str(latest["date"]),
        "sma_fast": round(fv, 4),
        "sma_slow": round(sv, 4),
    }


register("sma", requires=["daily"], signal_fn=_sma_signal)
register("portfolio_sma", requires=["daily"], signal_fn=_sma_signal)
register("credit_spread", requires=["options_snapshot"], signal_fn=lambda t, p: {"ticker": t, "signal": "HOLD", "reason": "options_manual"})
