"""Shared pytest fixtures for DeltaStack tests.

Sets up a temporary data directory and in-memory DuckDB so tests never touch
production data or make network calls.
"""

from __future__ import annotations

import os
import json
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

# ── override env BEFORE any deltastack import ───────────────────────────────
_tmp = tempfile.mkdtemp(prefix="ds_test_")
os.environ["DATA_DIR"] = _tmp
os.environ["DB_PATH"] = str(Path(_tmp) / "test.duckdb")
os.environ["DELTASTACK_API_KEY"] = "test-key-12345"
os.environ["MASSIVE_API_KEY"] = "fake-key"
os.environ["TRADING_ENABLED"] = "false"
os.environ["LOG_LEVEL"] = "WARNING"

# Now safe to import
from deltastack.config import get_settings, Settings  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    """Clear the lru_cache so each test gets fresh settings."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def tmp_data_dir() -> Path:
    return Path(_tmp)


@pytest.fixture
def golden_bars_df() -> pd.DataFrame:
    """20-row golden sample of daily bars for deterministic testing."""
    base = date(2025, 1, 2)
    rows = []
    price = 150.0
    for i in range(20):
        d = base + timedelta(days=i)
        # Skip weekends for realism
        if d.weekday() >= 5:
            continue
        o = price + (i % 3) * 0.5
        h = o + 1.0
        l = o - 0.5
        c = o + 0.3
        rows.append({
            "date": d,
            "open": round(o, 2),
            "high": round(h, 2),
            "low": round(l, 2),
            "close": round(c, 2),
            "volume": 1_000_000 + i * 10_000,
            "vwap": round(o + 0.1, 2),
            "trades": 5000 + i * 100,
            "adjusted": True,
        })
    return pd.DataFrame(rows)


@pytest.fixture
def stored_ticker(golden_bars_df, tmp_data_dir) -> str:
    """Save golden bars as TEST ticker and return the ticker name."""
    from deltastack.data.storage import save_bars
    save_bars("TEST", golden_bars_df)
    return "TEST"


@pytest.fixture
def db_ready(tmp_data_dir):
    """Ensure DuckDB tables exist."""
    from deltastack.db import ensure_tables
    ensure_tables()
    return True


@pytest.fixture
def app_client():
    """FastAPI TestClient with test API key."""
    from fastapi.testclient import TestClient
    from api.main import app
    # Ensure DB tables
    from deltastack.db import ensure_tables
    ensure_tables()
    return TestClient(app)
