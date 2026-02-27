"""Tests for Phase H: orchestration, WFA, risk engine, alerts, dashboard."""

import pytest
from unittest.mock import patch

API_KEY = "test-key-12345"
HEADERS = {"X-API-Key": API_KEY}


class TestOrchestration:
    def test_dry_run_returns_batch_id(self, app_client, stored_ticker):
        # Ensure universe file points to test ticker
        from pathlib import Path
        from deltastack.config import get_settings
        settings = get_settings()
        universe_path = Path(settings.universe_file)
        universe_path.parent.mkdir(parents=True, exist_ok=True)
        universe_path.write_text(f"{stored_ticker}\n")

        r = app_client.post(
            "/orchestrate/daily",
            json={
                "date": "2025-01-15",
                "strategies": [{"name": "sma", "params": {"fast": 3, "slow": 5}}],
                "mode": "dry_run",
            },
            headers=HEADERS,
        )
        assert r.status_code == 200
        data = r.json()
        assert "batch_id" in data
        assert data["status"] == "completed"
        assert data["mode"] == "dry_run"

    def test_auto_confirm_blocked_by_default(self, app_client):
        r = app_client.post(
            "/orchestrate/daily",
            json={
                "date": "2025-01-15",
                "strategies": [{"name": "sma", "params": {}}],
                "mode": "dry_run",
                "auto_confirm": True,
            },
            headers=HEADERS,
        )
        assert r.status_code == 403


class TestRiskEngine:
    def test_evaluate_plan_returns_accepted(self, app_client, stored_ticker):
        r = app_client.post(
            "/risk/evaluate_plan",
            json={"orders": [{"ticker": stored_ticker, "side": "BUY", "qty": 1}]},
            headers=HEADERS,
        )
        assert r.status_code == 200
        data = r.json()
        assert "accepted" in data
        assert "limits" in data


class TestDashboard:
    def test_dashboard_summary(self, app_client):
        r = app_client.get("/dashboard/summary", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert "data_freshness" in data
        assert "broker" in data
        assert "recent_errors" in data


class TestAlerts:
    def test_alert_redaction(self):
        from deltastack.alerts import _redact
        data = {"api_key": "secret123", "ticker": "AAPL", "access_token": "tok"}
        redacted = _redact(data)
        assert redacted["api_key"] == "***REDACTED***"
        assert redacted["access_token"] == "***REDACTED***"
        assert redacted["ticker"] == "AAPL"

    def test_alert_test_endpoint_no_webhook(self, app_client):
        r = app_client.post("/ops/alert/test", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert data["webhook_configured"] is False
