"""Tests for execution workflow and portfolio report."""

import os
import pytest

API_KEY = "test-key-12345"
HEADERS = {"X-API-Key": API_KEY}


class TestExecutionPlan:
    """Execution plan endpoints."""

    def test_create_plan_returns_plan_id(self, app_client, stored_ticker):
        r = app_client.post(
            "/execute/plan",
            json={"strategy": "sma", "ticker": stored_ticker, "side": "BUY", "qty": 5},
            headers=HEADERS,
        )
        assert r.status_code == 200
        data = r.json()
        assert "plan_id" in data
        assert data["status"] == "pending"
        assert len(data["orders"]) == 1
        assert "risk_summary" in data

    def test_confirm_blocked_when_trading_disabled(self, app_client, stored_ticker):
        # Create a plan first
        r1 = app_client.post(
            "/execute/plan",
            json={"strategy": "sma", "ticker": stored_ticker, "side": "BUY", "qty": 5},
            headers=HEADERS,
        )
        plan_id = r1.json()["plan_id"]

        # Confirm should fail (TRADING_ENABLED=false in test env)
        r2 = app_client.post(
            "/execute/confirm",
            json={"plan_id": plan_id},
            headers=HEADERS,
        )
        assert r2.status_code == 503
        assert "disabled" in r2.json()["detail"].lower()


class TestPortfolioReport:
    def test_portfolio_report_schema(self, app_client, stored_ticker):
        r = app_client.get("/portfolio/report", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert "account" in data
        assert "positions" in data
        assert "pnl" in data
        assert "num_positions" in data

    def test_backtest_report_missing_run(self, app_client):
        r = app_client.get("/portfolio/backtest/nonexistent", headers=HEADERS)
        assert r.status_code == 404


class TestOptionsGreeksEndpoint:
    def test_greeks_call(self, app_client):
        r = app_client.post(
            "/options/greeks",
            json={"spot": 100, "strike": 100, "tte_years": 0.25, "iv": 0.2, "option_type": "call"},
            headers=HEADERS,
        )
        assert r.status_code == 200
        data = r.json()
        assert 0 < data["delta"] < 1
        assert data["price"] > 0
