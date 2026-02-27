"""Tests for 0DTE features: intraday options, flatten, mad max seed."""

import pytest
from datetime import date

import pandas as pd

API_KEY = "test-key-12345"
HEADERS = {"X-API-Key": API_KEY}


class TestIntradayOptionsStorage:
    def test_save_and_load_snapshot(self, tmp_data_dir, db_ready):
        from deltastack.ingest.options_intraday import save_intraday_snapshot, load_intraday_snapshot

        df = pd.DataFrame([
            {"ticker": "SPY260206P00580000", "underlying": "SPY", "type": "put",
             "strike": 580, "expiration": "2026-02-06", "bid": 1.0, "ask": 1.5,
             "volume": 100, "open_interest": 500, "iv": 0.20, "delta": -0.20},
            {"ticker": "SPY260206P00575000", "underlying": "SPY", "type": "put",
             "strike": 575, "expiration": "2026-02-06", "bid": 0.5, "ask": 0.8,
             "volume": 200, "open_interest": 300, "iv": 0.22, "delta": -0.12},
        ])
        save_intraday_snapshot("SPY", date(2026, 2, 6), "1030", df)
        loaded = load_intraday_snapshot("SPY", date(2026, 2, 6), "1030")
        assert len(loaded) == 2

    def test_list_available_times(self, tmp_data_dir, db_ready):
        from deltastack.ingest.options_intraday import save_intraday_snapshot, list_available_times

        df = pd.DataFrame([
            {"ticker": "SPY", "underlying": "SPY", "type": "put", "strike": 580,
             "expiration": "2026-02-06", "bid": 1.0, "ask": 1.5, "volume": 100},
        ])
        save_intraday_snapshot("SPY", date(2026, 2, 6), "1035", df)
        times = list_available_times("SPY", date(2026, 2, 6))
        assert any(t["time"] == "1035" for t in times)


class TestMadMaxSeed0DTE:
    def test_seed_includes_0dte_strategy_with_qqq(self, db_ready):
        import json
        from deltastack.db.dao_agents import seed_mad_max, get_agent_strategies
        agent_id = seed_mad_max()
        strategies = get_agent_strategies(agent_id)
        dte_strat = [s for s in strategies if s["strategy_name"] == "0dte_credit_spread"]
        assert len(dte_strat) == 1
        params = json.loads(dte_strat[0]["params_json"]) if isinstance(dte_strat[0]["params_json"], str) else dte_strat[0]["params_json"]
        assert params["underlying"] == "QQQ"
        assert params["width"] == 2
        assert params["entry_end"] == "1415"


class TestFlattenEndpoint:
    def test_flatten_blocked_when_trading_disabled(self, app_client):
        from deltastack.db.dao_agents import seed_mad_max
        agent_id = seed_mad_max()
        r = app_client.post(f"/agents/{agent_id}/flatten", headers=HEADERS)
        assert r.status_code == 503
        assert "disabled" in r.json()["detail"].lower()


class TestZeroDTERiskCaps:
    def test_0dte_config_defaults(self):
        from deltastack.config import get_settings
        s = get_settings()
        assert s.max_0dte_trades_per_day == 5
        assert s.max_0dte_notional_per_day == 20_000
        assert s.max_0dte_daily_loss == 1_500
        assert s.max_0dte_position_minutes == 45


class TestZeroDTEEndpoints:
    def test_intraday_snapshot_endpoint(self, app_client):
        r = app_client.get(
            "/options/chain_intraday/SPY?date=2025-01-02&time=1030",
            headers=HEADERS,
        )
        assert r.status_code in (404, 200)

    def test_0dte_backtest_endpoint_no_data(self, app_client):
        r = app_client.post(
            "/options/backtest/0dte_credit_spread",
            json={"underlying": "SPY", "date": "2025-01-02"},
            headers=HEADERS,
        )
        assert r.status_code in (400, 404, 500)

    def test_curve_endpoint_missing(self, app_client):
        r = app_client.get("/options/backtest/nonexistent/curve", headers=HEADERS)
        assert r.status_code == 404
