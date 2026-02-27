"""CLI for agent operations.

Usage:
    python -m deltastack.agent run --agent mad_max --date 2026-02-06 --dry-run
    python -m deltastack.agent run --agent mad_max
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from deltastack.config import get_settings
from deltastack.db import ensure_tables


def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )
    ensure_tables()

    # Seed mad_max on every CLI run
    from deltastack.db.dao_agents import seed_mad_max
    seed_mad_max()

    parser = argparse.ArgumentParser(description="DeltaStack Agent CLI")
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Run an agent")
    run_p.add_argument("--agent", required=True, help="Agent name")
    run_p.add_argument("--date", default=None, help="Run date YYYY-MM-DD")
    run_p.add_argument("--dry-run", action="store_true", help="Signal only, no plans")

    args = parser.parse_args()

    if args.command == "run":
        from deltastack.agent.runner import run_agent
        run_date = date.fromisoformat(args.date) if args.date else date.today()
        result = run_agent(args.agent, run_date=run_date, dry_run=args.dry_run)
        print(json.dumps(result, indent=2, default=str))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
