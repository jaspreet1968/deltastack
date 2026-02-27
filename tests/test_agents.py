"""Tests for agent CRUD, dashboard, runner, and seed."""

import pytest

API_KEY = "test-key-12345"
HEADERS = {"X-API-Key": API_KEY}


class TestAgentCRUD:
    def test_create_agent(self, app_client):
        r = app_client.post(
            "/agents",
            json={"name": "test_agent", "display_name": "Test Agent", "risk_profile": "BALANCED"},
            headers=HEADERS,
        )
        assert r.status_code == 200
        data = r.json()
        assert "agent_id" in data
        assert data["name"] == "test_agent"

    def test_list_agents(self, app_client):
        r = app_client.get("/agents", headers=HEADERS)
        assert r.status_code == 200
        assert "agents" in r.json()

    def test_get_agent_not_found(self, app_client):
        r = app_client.get("/agents/nonexistent", headers=HEADERS)
        assert r.status_code == 404

    def test_patch_agent(self, app_client):
        # Create first
        r1 = app_client.post(
            "/agents",
            json={"name": "patch_test", "display_name": "Before"},
            headers=HEADERS,
        )
        agent_id = r1.json()["agent_id"]

        r2 = app_client.patch(
            f"/agents/{agent_id}",
            json={"display_name": "After"},
            headers=HEADERS,
        )
        assert r2.status_code == 200
        assert "display_name" in r2.json()["updated"]


class TestAgentStrategies:
    def test_add_strategy(self, app_client):
        r1 = app_client.post(
            "/agents",
            json={"name": "strat_test"},
            headers=HEADERS,
        )
        agent_id = r1.json()["agent_id"]

        r2 = app_client.post(
            f"/agents/{agent_id}/strategies",
            json={"strategy_name": "sma", "params": {"fast": 10, "slow": 30}},
            headers=HEADERS,
        )
        assert r2.status_code == 200
        assert "agent_strategy_id" in r2.json()


class TestAgentDashboard:
    def test_dashboard_returns_schema(self, app_client):
        r1 = app_client.post(
            "/agents",
            json={"name": "dash_test"},
            headers=HEADERS,
        )
        agent_id = r1.json()["agent_id"]

        r2 = app_client.get(f"/agents/{agent_id}/dashboard", headers=HEADERS)
        assert r2.status_code == 200
        data = r2.json()
        assert "agent" in data
        assert "strategies" in data
        assert "recent_runs" in data
        assert "signals" in data


class TestAgentRunner:
    def test_run_agent(self, app_client, stored_ticker):
        from pathlib import Path
        from deltastack.config import get_settings
        settings = get_settings()
        universe_path = Path(settings.universe_file)
        universe_path.parent.mkdir(parents=True, exist_ok=True)
        universe_path.write_text(f"{stored_ticker}\n")

        # Create agent with strategy
        r1 = app_client.post(
            "/agents",
            json={"name": "run_test"},
            headers=HEADERS,
        )
        agent_id = r1.json()["agent_id"]
        app_client.post(
            f"/agents/{agent_id}/strategies",
            json={"strategy_name": "sma", "params": {"fast": 3, "slow": 5}},
            headers=HEADERS,
        )

        r2 = app_client.post(
            f"/agents/{agent_id}/run",
            json={"mode": "signal"},
            headers=HEADERS,
        )
        assert r2.status_code == 200
        data = r2.json()
        assert data["strategies_run"] >= 1


class TestMadMaxSeed:
    def test_seed_creates_mad_max(self, db_ready):
        from deltastack.db.dao_agents import seed_mad_max, get_agent_by_name
        agent_id = seed_mad_max()
        assert agent_id
        agent = get_agent_by_name("mad_max")
        assert agent is not None
        assert agent["risk_profile"] == "SUPER_RISKY"

    def test_seed_idempotent(self, db_ready):
        from deltastack.db.dao_agents import seed_mad_max
        id1 = seed_mad_max()
        id2 = seed_mad_max()
        assert id1 == id2


class TestMarketHours:
    def test_market_hours_function_exists(self):
        from deltastack.agent.runner import is_market_hours
        result = is_market_hours()
        assert isinstance(result, bool)
