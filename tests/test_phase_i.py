"""Tests for Phase I: tick runner, replay, promotion, snapshot status."""

import json
from datetime import date

import pandas as pd
import pytest

API_KEY = "test-key-12345"
HEADERS = {"X-API-Key": API_KEY}


class TestTickRunner:
    def test_tick_with_no_snapshots(self, app_client, db_ready):
        from deltastack.db.dao_agents import seed_mad_max
        agent_id = seed_mad_max()
        r = app_client.post(
            f"/agents/{agent_id}/tick",
            json={"date": "2026-02-06", "time": "1030"},
            headers=HEADERS,
        )
        assert r.status_code == 200
        data = r.json()
        assert data["decision"] in ("skip", "error")


class TestReplay:
    def test_replay_with_no_data(self, app_client, db_ready):
        from deltastack.db.dao_agents import seed_mad_max
        agent_id = seed_mad_max()
        r = app_client.post(
            f"/agents/{agent_id}/replay",
            json={"date": "2026-02-06", "start_time": "1000", "end_time": "1100"},
            headers=HEADERS,
        )
        assert r.status_code == 200
        data = r.json()
        assert "replay_id" in data
        assert "timeline" in data


class TestPromotion:
    def test_promote_strategy(self, app_client, db_ready):
        from deltastack.db.dao_agents import seed_mad_max, get_agent_strategies
        agent_id = seed_mad_max()
        strategies = get_agent_strategies(agent_id)
        sid = strategies[0]["agent_strategy_id"]

        r = app_client.patch(
            f"/agents/{agent_id}/strategies/{sid}/status",
            json={"status": "paper_live", "reason": "testing"},
            headers=HEADERS,
        )
        assert r.status_code == 200
        assert r.json()["new_status"] == "paper_live"

    def test_invalid_status_rejected(self, app_client, db_ready):
        from deltastack.db.dao_agents import seed_mad_max, get_agent_strategies
        agent_id = seed_mad_max()
        strategies = get_agent_strategies(agent_id)
        sid = strategies[0]["agent_strategy_id"]

        r = app_client.patch(
            f"/agents/{agent_id}/strategies/{sid}/status",
            json={"status": "LIVE_REAL_MONEY"},
            headers=HEADERS,
        )
        assert r.status_code == 400

    def test_strategy_history(self, app_client, db_ready):
        from deltastack.db.dao_agents import seed_mad_max, get_agent_strategies
        agent_id = seed_mad_max()
        strategies = get_agent_strategies(agent_id)
        sid = strategies[0]["agent_strategy_id"]

        # Promote first
        app_client.patch(
            f"/agents/{agent_id}/strategies/{sid}/status",
            json={"status": "paper_live"},
            headers=HEADERS,
        )

        r = app_client.get(f"/agents/{agent_id}/strategies/{sid}/history", headers=HEADERS)
        assert r.status_code == 200
        assert len(r.json()["events"]) >= 1


class TestSnapshotStatus:
    def test_snapshot_status_endpoint(self, app_client, db_ready):
        r = app_client.get("/options/snapshots_intraday/status?underlying=QQQ&date=2026-02-06", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert "captured_count" in data
        assert "gaps" in data
