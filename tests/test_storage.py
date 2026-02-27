"""Unit tests for deltastack/data/storage.py – upsert, dedup, monotonic."""

import pandas as pd
import pytest
from datetime import date, timedelta

from deltastack.data.storage import save_bars, load_bars, read_metadata


class TestUpsertBehavior:
    """Verify that overlapping inserts produce clean data."""

    def test_no_duplicate_dates(self, golden_bars_df, tmp_data_dir):
        save_bars("DUP", golden_bars_df)
        # Save again with same data
        save_bars("DUP", golden_bars_df)
        df = load_bars("DUP", limit=100)
        assert df["date"].duplicated().sum() == 0

    def test_overlapping_ranges_merge(self, tmp_data_dir):
        rows_a = [
            {"date": date(2025, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100.5, "volume": 1000},
            {"date": date(2025, 1, 3), "open": 101, "high": 102, "low": 100, "close": 101.5, "volume": 2000},
        ]
        rows_b = [
            {"date": date(2025, 1, 3), "open": 101, "high": 102, "low": 100, "close": 102.0, "volume": 2500},
            {"date": date(2025, 1, 6), "open": 102, "high": 103, "low": 101, "close": 102.5, "volume": 3000},
        ]
        save_bars("OVR", pd.DataFrame(rows_a))
        save_bars("OVR", pd.DataFrame(rows_b))
        df = load_bars("OVR", limit=100)
        # Should have 3 unique dates
        assert len(df) == 3
        # Jan 3 should have the LATEST value (close=102.0)
        jan3 = df[df["date"] == date(2025, 1, 3)]
        assert float(jan3.iloc[0]["close"]) == 102.0

    def test_monotonic_dates_after_upsert(self, golden_bars_df, tmp_data_dir):
        save_bars("MONO", golden_bars_df)
        df = load_bars("MONO", limit=100)
        dates = list(df["date"])
        assert dates == sorted(dates), "Dates are not monotonically increasing"

    def test_metadata_written(self, golden_bars_df, tmp_data_dir):
        save_bars("META", golden_bars_df)
        meta = read_metadata("META")
        assert meta is not None
        assert meta["ticker"] == "META"
        assert meta["rows"] > 0
        assert "updated_utc" in meta


class TestLoadBars:
    def test_date_filter(self, stored_ticker, tmp_data_dir):
        df = load_bars(stored_ticker, start=date(2025, 1, 6), end=date(2025, 1, 10))
        for d in df["date"]:
            assert d >= date(2025, 1, 6)
            assert d <= date(2025, 1, 10)

    def test_limit_offset(self, stored_ticker, tmp_data_dir):
        df_all = load_bars(stored_ticker, limit=100)
        df_page = load_bars(stored_ticker, limit=3, offset=2)
        assert len(df_page) <= 3

    def test_missing_ticker_raises(self, tmp_data_dir):
        with pytest.raises(FileNotFoundError):
            load_bars("NOSUCH")
