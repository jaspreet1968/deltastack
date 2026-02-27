"""Tests for Phase G: intraday, orders, signals, data freshness."""

import os
from datetime import date

import pandas as pd
import pytest

API_KEY = "test-key-12345"
HEADERS = {"X-API-Key": API_KEY}


class TestIntradayStorage:
    def test_save_and_load_intraday(self, tmp_data_dir):
        from deltastack.data.intraday import save_intraday, load_intraday

        df = pd.DataFrame([
            {"timestamp": "2025-01-02T09:30:00+00:00", "open": 150, "high": 151, "low": 149, "close": 150.5, "volume": 1000},
            {"timestamp": "2025-01-02T09:35:00+00:00", "open": 150.5, "high": 152, "low": 150, "close": 151.5, "volume": 2000},
        ])
        bar_date = date(2025, 1, 2)
        save_intraday("INTRA", bar_date, df)
        loaded = load_intraday("INTRA", bar_date)
        assert len(loaded) == 2

    def test_load_missing_raises(self, tmp_data_dir):
        from deltastack.data.intraday import load_intraday
        with pytest.raises(FileNotFoundError):
            load_intraday("NOSUCH", date(2025, 1, 2))


class TestIntradayEndpoint:
    def test_intraday_missing_returns_404(self, app_client):
        r = app_client.get("/intraday/NOSUCH?date=2025-01-02", headers=HEADERS)
        assert r.status_code == 404


class TestOrdersEndpoint:
    def test_list_orders_empty(self, app_client):
        r = app_client.get("/orders", headers=HEADERS)
        assert r.status_code == 200
        assert "orders" in r.json()

    def test_get_missing_order_404(self, app_client):
        r = app_client.get("/orders/nonexistent", headers=HEADERS)
        assert r.status_code == 404


class TestSignalsUniverse:
    def test_run_universe_with_stored_data(self, app_client, stored_ticker):
        # Create a universe file pointing to our test ticker
        import tempfile
        from pathlib import Path
        from deltastack.config import get_settings
        settings = get_settings()
        universe_path = Path(settings.universe_file)
        universe_path.parent.mkdir(parents=True, exist_ok=True)
        universe_path.write_text(f"{stored_ticker}\n")

        r = app_client.post("/signals/run_universe", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert "batch_id" in data
        assert data["count"] >= 1

    def test_latest_signal_missing(self, app_client):
        r = app_client.get("/signals/latest?ticker=NOSUCH", headers=HEADERS)
        assert r.status_code == 404


class TestDataFreshness:
    def test_freshness_endpoint(self, app_client):
        r = app_client.get("/data/freshness", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert "daily_bars_last_updated" in data
        assert "intraday_bars_last_updated" in data


class TestOpsErrors:
    def test_ops_errors_endpoint(self, app_client):
        r = app_client.get("/ops/errors", headers=HEADERS)
        assert r.status_code == 200
        assert "errors" in r.json()


class TestIdempotency:
    def test_plan_creation_works(self, app_client, stored_ticker):
        r = app_client.post(
            "/execute/plan",
            json={"strategy": "test", "ticker": stored_ticker, "side": "BUY", "qty": 1},
            headers=HEADERS,
        )
        assert r.status_code == 200
        assert r.json()["status"] == "pending"
