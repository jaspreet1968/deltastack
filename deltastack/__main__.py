"""CLI entry-point for DeltaStack data ingestion.

Usage examples::

    # Single or comma-separated tickers
    python -m deltastack --ticker AAPL --start 2020-01-01 --end 2026-01-01

    # Multiple tickers
    python -m deltastack --ticker AAPL,MSFT,GOOGL --start 2016-01-01 --end 2026-01-01

    # From a file (one ticker per line)
    python -m deltastack --ticker-file config/universe.txt --start 2016-01-01 --end 2026-01-01
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

from deltastack.config import get_settings
from deltastack.ingest.polygon import fetch_daily_bars, fetch_batch


def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(description="DeltaStack CLI ingestion")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--ticker", help="Comma-separated tickers (e.g. AAPL,MSFT)")
    group.add_argument("--ticker-file", help="Path to a file with one ticker per line")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--force", action="store_true", help="Force re-download")
    args = parser.parse_args()

    # Resolve ticker list
    if args.ticker_file:
        path = Path(args.ticker_file)
        if not path.exists():
            print(f"ERROR: ticker file not found: {path}", file=sys.stderr)
            sys.exit(1)
        tickers = [
            line.strip().upper()
            for line in path.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
    else:
        tickers = [t.strip().upper() for t in args.ticker.split(",") if t.strip()]

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    if len(tickers) == 1:
        result = fetch_daily_bars(tickers[0], start, end, force=args.force)
        print(json.dumps(result, indent=2))
    else:
        results = fetch_batch(tickers, start, end, force=args.force)
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
