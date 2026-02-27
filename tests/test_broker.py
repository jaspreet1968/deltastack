"""Tests for broker mode enforcement and factory logic (no network)."""

import os
import pytest

from deltastack.broker.factory import get_broker_status, _validate_secrets, reset_broker


class TestBrokerModeEnforcement:
    """Verify paper-only safety checks."""

    def test_default_provider_is_paper(self):
        status = get_broker_status()
        assert status["provider"] == "paper"
        assert status["mode"] == "paper"
        assert status["ok"] is True

    def test_live_mode_blocked(self, monkeypatch):
        monkeypatch.setenv("BROKER_MODE", "live")
        from deltastack.config import get_settings
        get_settings.cache_clear()
        err = _validate_secrets()
        assert err is not None
        assert "paper" in err.lower()
        monkeypatch.setenv("BROKER_MODE", "paper")
        get_settings.cache_clear()

    def test_alpaca_placeholder_blocked(self, monkeypatch):
        monkeypatch.setenv("BROKER_PROVIDER", "alpaca")
        monkeypatch.setenv("ALPACA_API_KEY", "your_polygon_api_key_here")
        monkeypatch.setenv("ALPACA_SECRET_KEY", "test")
        from deltastack.config import get_settings
        get_settings.cache_clear()
        err = _validate_secrets()
        assert err is not None
        assert "placeholder" in err.lower()
        monkeypatch.setenv("BROKER_PROVIDER", "paper")
        get_settings.cache_clear()

    def test_tradier_placeholder_blocked(self, monkeypatch):
        monkeypatch.setenv("BROKER_PROVIDER", "tradier")
        monkeypatch.setenv("TRADIER_ACCESS_TOKEN", "")
        from deltastack.config import get_settings
        get_settings.cache_clear()
        err = _validate_secrets()
        assert err is not None
        assert "tradier" in err.lower()
        monkeypatch.setenv("BROKER_PROVIDER", "paper")
        get_settings.cache_clear()

    def test_tradestation_placeholder_blocked(self, monkeypatch):
        monkeypatch.setenv("BROKER_PROVIDER", "tradestation")
        monkeypatch.setenv("TRADESTATION_CLIENT_ID", "")
        monkeypatch.setenv("TRADESTATION_CLIENT_SECRET", "test")
        from deltastack.config import get_settings
        get_settings.cache_clear()
        err = _validate_secrets()
        assert err is not None
        assert "tradestation" in err.lower()
        monkeypatch.setenv("BROKER_PROVIDER", "paper")
        get_settings.cache_clear()


class TestURLSafetyChecks:
    """Verify sandbox/paper/SIM URL detection."""

    def test_alpaca_paper_url(self):
        from deltastack.broker.alpaca import _is_paper_url
        assert _is_paper_url("https://paper-api.alpaca.markets") is True
        assert _is_paper_url("https://api.alpaca.markets") is False
        assert _is_paper_url("https://broker-api.sandbox.alpaca.markets") is True

    def test_tradier_sandbox_url(self):
        from deltastack.broker.tradier import _is_sandbox_url
        assert _is_sandbox_url("https://sandbox.tradier.com/v1") is True
        assert _is_sandbox_url("https://api.tradier.com/v1") is False

    def test_tradestation_sim_url(self):
        from deltastack.broker.tradestation import _is_sim_url
        assert _is_sim_url("https://sim-api.tradestation.com/v3") is True
        assert _is_sim_url("https://api.tradestation.com/v3") is False


class TestBrokerStatusEndpoint:
    def test_broker_status_endpoint(self, app_client):
        HEADERS = {"X-API-Key": "test-key-12345"}
        r = app_client.get("/broker/status", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert data["provider"] == "paper"
        assert data["ok"] is True

    def test_ops_status_endpoint(self, app_client):
        HEADERS = {"X-API-Key": "test-key-12345"}
        r = app_client.get("/ops/status", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert "uptime_seconds" in data
        assert "broker" in data

    def test_health_history_endpoint(self, app_client):
        HEADERS = {"X-API-Key": "test-key-12345"}
        r = app_client.get("/health/history", headers=HEADERS)
        assert r.status_code == 200
        assert "checks" in r.json()
