#!/usr/bin/env python3
"""Generate SMA signals for all tickers in the universe and persist to DB.

Called by deltastack-signals.timer daily at 00:30 UTC.
Usage: python scripts/generate_signals.py
"""

import json
import logging
import sys
from pathlib import Path

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from deltastack.config import get_settings
from deltastack.data.storage import load_bars, ticker_exists
from deltastack.db import ensure_tables
from deltastack.db.dao import insert_signal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("deltastack.signals")


def main() -> None:
    settings = get_settings()
    ensure_tables()

    universe_path = Path(settings.universe_file)
    if not universe_path.exists():
        logger.error("Universe file not found: %s", universe_path)
        sys.exit(1)

    tickers = [
        line.strip().upper()
        for line in universe_path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    fast, slow = 10, 30
    generated = 0

    for ticker in tickers:
        if not ticker_exists(ticker):
            logger.warning("No data for %s – skipping", ticker)
            continue

        try:
            df = load_bars(ticker, limit=100_000)
            if len(df) < slow + 1:
                continue

            df = df.sort_values("date").reset_index(drop=True)
            df["close_f"] = df["close"].astype(float)
            df["sma_fast"] = df["close_f"].rolling(window=fast).mean()
            df["sma_slow"] = df["close_f"].rolling(window=slow).mean()
            df = df.dropna(subset=["sma_fast", "sma_slow"])

            if len(df) < 2:
                continue

            latest = df.iloc[-1]
            prev = df.iloc[-2]
            fast_val = float(latest["sma_fast"])
            slow_val = float(latest["sma_slow"])

            if float(prev["sma_fast"]) <= float(prev["sma_slow"]) and fast_val > slow_val:
                signal = "BUY"
            elif float(prev["sma_fast"]) >= float(prev["sma_slow"]) and fast_val < slow_val:
                signal = "SELL"
            else:
                signal = "HOLD"

            insert_signal(
                strategy=f"sma_{fast}_{slow}",
                ticker=ticker,
                signal=signal,
                as_of=str(latest["date"]),
                meta={"fast": fast_val, "slow": slow_val, "close": float(latest["close_f"])},
            )
            generated += 1
            logger.info("%s %s: %s (fast=%.2f slow=%.2f)", ticker, latest["date"], signal, fast_val, slow_val)

        except Exception:
            logger.exception("Failed to generate signal for %s", ticker)

    logger.info("Generated %d signals for %d tickers", generated, len(tickers))


if __name__ == "__main__":
    main()
