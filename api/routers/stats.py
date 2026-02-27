"""GET /stats/storage – disk usage and storage summary."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from fastapi import APIRouter

from deltastack.config import get_settings
from deltastack.data.cache import get_bars_cache, get_options_cache

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/stats", tags=["stats"])


def _dir_size(path: Path) -> int:
    """Recursively compute total bytes under a directory."""
    total = 0
    if not path.exists():
        return 0
    for f in path.rglob("*"):
        if f.is_file():
            total += f.stat().st_size
    return total


def _count_tickers(bars_dir: Path) -> int:
    if not bars_dir.exists():
        return 0
    return sum(1 for d in bars_dir.iterdir() if d.is_dir() and d.name.startswith("ticker="))


def _count_options_snapshots(options_dir: Path) -> int:
    if not options_dir.exists():
        return 0
    count = 0
    for underlying_dir in options_dir.iterdir():
        if underlying_dir.is_dir():
            count += sum(1 for d in underlying_dir.iterdir() if d.is_dir())
    return count


@router.get("/storage")
def storage_stats():
    """Return disk usage, ticker count, options snapshot count, and DB size."""
    settings = get_settings()
    data_dir = Path(settings.data_dir)
    db_path = Path(settings.resolved_db_path)

    bars_size = _dir_size(settings.bars_dir)
    options_size = _dir_size(settings.options_dir)
    db_size = db_path.stat().st_size if db_path.exists() else 0

    return {
        "data_dir": str(data_dir),
        "bars_size_mb": round(bars_size / 1_048_576, 2),
        "options_size_mb": round(options_size / 1_048_576, 2),
        "db_size_mb": round(db_size / 1_048_576, 2),
        "total_size_mb": round((bars_size + options_size + db_size) / 1_048_576, 2),
        "tickers_stored": _count_tickers(settings.bars_dir),
        "options_snapshots": _count_options_snapshots(settings.options_dir),
        "bars_cache": get_bars_cache().stats(),
        "options_cache": get_options_cache().stats(),
    }
