"""API integration tests using FastAPI TestClient (no network calls)."""

import pytest


API_KEY = "test-key-12345"
HEADERS = {"X-API-Key": API_KEY}


class TestHealthAndDocs:
    """Public endpoints that require no auth."""

    def test_health_no_auth(self, app_client):
        r = app_client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_docs_no_auth(self, app_client):
        r = app_client.get("/docs")
        assert r.status_code == 200

    def test_metrics_no_auth(self, app_client):
        r = app_client.get("/metrics/basic")
        assert r.status_code == 200
        data = r.json()
        assert "uptime_seconds" in data


class TestAuthRequired:
    """Protected endpoints must reject requests without a valid key."""

    def test_prices_no_key_returns_401(self, app_client):
        r = app_client.get("/prices/AAPL")
        assert r.status_code == 401

    def test_prices_wrong_key_returns_401(self, app_client):
        r = app_client.get("/prices/AAPL", headers={"X-API-Key": "wrong"})
        assert r.status_code == 401

    def test_ingest_no_key_returns_401(self, app_client):
        r = app_client.post("/ingest/daily", json={"ticker": "AAPL", "start": "2025-01-01", "end": "2025-02-01"})
        assert r.status_code == 401


class TestTradingKillSwitch:
    """Trading endpoints must return 503 when TRADING_ENABLED is false."""

    def test_trade_order_blocked(self, app_client):
        r = app_client.post(
            "/trade/order",
            json={"ticker": "AAPL", "side": "BUY", "qty": 10},
            headers=HEADERS,
        )
        assert r.status_code == 503
        assert "disabled" in r.json()["detail"].lower()

    def test_trade_positions_blocked(self, app_client):
        r = app_client.get("/trade/positions", headers=HEADERS)
        assert r.status_code == 503

    def test_trade_account_blocked(self, app_client):
        r = app_client.get("/trade/account", headers=HEADERS)
        assert r.status_code == 503


class TestPricesAndBacktest:
    """Test data retrieval and backtest endpoints with stored fixture data."""

    def test_prices_with_stored_data(self, app_client, stored_ticker):
        r = app_client.get(f"/prices/{stored_ticker}?limit=5", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert data["count"] > 0
        assert len(data["bars"]) <= 5

    def test_prices_missing_ticker_404(self, app_client):
        r = app_client.get("/prices/NOSUCH", headers=HEADERS)
        assert r.status_code == 404

    def test_backtest_sma_with_stored_data(self, app_client, stored_ticker):
        r = app_client.post(
            "/backtest/sma",
            json={"ticker": stored_ticker, "start": "2025-01-01", "end": "2025-12-31", "fast": 3, "slow": 5},
            headers=HEADERS,
        )
        # May return 400 if not enough data for slow SMA, which is expected
        assert r.status_code in (200, 400)

    def test_data_status(self, app_client, stored_ticker):
        r = app_client.get(f"/data/status/{stored_ticker}", headers=HEADERS)
        assert r.status_code == 200
        assert r.json()["ticker"] == stored_ticker

    def test_greeks_endpoint(self, app_client):
        r = app_client.post(
            "/options/greeks",
            json={"spot": 100, "strike": 100, "tte_years": 0.25, "iv": 0.2, "option_type": "call"},
            headers=HEADERS,
        )
        assert r.status_code == 200
        data = r.json()
        assert 0 < data["delta"] < 1
        assert data["gamma"] > 0
