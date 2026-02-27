"""Centralised configuration via pydantic-settings.

Reads from environment variables (and optionally a .env file located next to
the project root).  The systemd unit should point ``EnvironmentFile=`` at the
same ``.env`` so that production picks up the values without touching code.
"""

from __future__ import annotations

import os
from pathlib import Path
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


_PROJECT_DIR = Path(__file__).resolve().parent.parent  # …/deltastack repo root


class Settings(BaseSettings):
    """Application-wide settings – all values come from env vars."""

    model_config = SettingsConfigDict(
        env_file=str(_PROJECT_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── API keys ──────────────────────────────────────────────────────
    massive_api_key: str = ""
    deltastack_api_key: str = ""

    # ── Paths ─────────────────────────────────────────────────────────
    data_dir: str = "/home/ec2-user/data/deltastack"
    universe_file: str = str(_PROJECT_DIR / "config" / "universe.txt")
    backup_dir: str = ""  # default: {data_dir}/backups

    # ── Database ──────────────────────────────────────────────────────
    db_path: str = ""  # default: {data_dir}/deltastack.duckdb

    # ── Logging ───────────────────────────────────────────────────────
    log_level: str = "INFO"

    # ── Misc ──────────────────────────────────────────────────────────
    default_bar_limit: int = 10_000

    # ── Rate limiting (requests per minute) ───────────────────────────
    ingest_rpm: int = 30
    backtest_rpm: int = 60
    trade_rpm: int = 30
    options_rpm: int = 30

    # ── Batch ingestion ───────────────────────────────────────────────
    max_batch_workers: int = 4

    # ── Data quality ──────────────────────────────────────────────────
    gap_warn_days: int = 7

    # ── HTTP retry / backoff ──────────────────────────────────────────
    http_max_retries: int = 3
    http_backoff_base: float = 1.0       # seconds; exponential: base * 2^attempt
    http_timeout: int = 30

    # ── Read cache ────────────────────────────────────────────────────
    cache_ttl_seconds: int = 60
    cache_max_size: int = 256

    # ── Trading kill switch ───────────────────────────────────────────
    trading_enabled: bool = False

    # ── Broker ────────────────────────────────────────────────────────
    broker_provider: str = "paper"       # paper | alpaca
    broker_mode: str = "paper"           # MUST be "paper" – live blocked

    # ── Alpaca (paper only) ───────────────────────────────────────────
    alpaca_api_key: str = ""
    alpaca_secret_key: str = ""
    alpaca_base_url: str = "https://paper-api.alpaca.markets"

    # ── Tradier (sandbox only) ────────────────────────────────────────
    tradier_access_token: str = ""
    tradier_base_url: str = "https://sandbox.tradier.com/v1"

    # ── TradeStation (SIM only) ───────────────────────────────────────
    tradestation_client_id: str = ""
    tradestation_client_secret: str = ""
    tradestation_base_url: str = "https://sim-api.tradestation.com/v3"

    # ── Paper broker defaults ─────────────────────────────────────────
    default_commission: float = 1.0
    default_slippage_bps: float = 2.0
    paper_initial_cash: float = 100_000.0

    # ── Risk controls (paper trading) ─────────────────────────────────
    max_notional_per_order: float = 50_000.0
    max_daily_orders: int = 100
    max_open_positions: int = 20
    max_daily_loss: float = 10_000.0     # paper P&L

    # ── Execution workflow ─────────────────────────────────────────────
    execution_require_confirm: bool = True   # plans must be confirmed before execution

    # ── Options trading defaults ──────────────────────────────────────
    options_slippage_pct: float = 0.01       # 1% slippage on options mid price
    options_contract_multiplier: int = 100   # shares per contract

    # ── Orchestration ─────────────────────────────────────────────────
    orchestration_auto_confirm_allowed: bool = False

    # ── Portfolio risk engine ─────────────────────────────────────────
    max_gross_exposure_pct: float = 1.0
    max_net_exposure_pct: float = 0.6
    max_single_ticker_exposure_pct: float = 0.2
    corr_lookback_days: int = 90
    max_correlated_bucket_exposure_pct: float = 0.4
    vol_target_annualized: float = 0.15

    # ── Alerts ────────────────────────────────────────────────────────
    alert_webhook_url: str = ""
    alert_level: str = "INFO"

    # ── 0DTE risk caps (hard limits, cannot be bypassed by agents) ───
    max_0dte_trades_per_day: int = 5
    max_0dte_notional_per_day: float = 20_000.0
    max_0dte_daily_loss: float = 1_500.0
    max_0dte_position_minutes: int = 45

    # ── Intraday data ────────────────────────────────────────────────
    intraday_timespan: str = "minute"
    intraday_multiplier: int = 5
    intraday_max_days_back: int = 7

    # ── Market hours ──────────────────────────────────────────────────
    market_timezone: str = "America/New_York"
    market_open: str = "09:30"
    market_close: str = "16:00"

    # ── Signals ───────────────────────────────────────────────────────
    signals_batch_size: int = 50

    # ── Risk-free rate for greeks ─────────────────────────────────────
    risk_free_rate: float = 0.05

    # ── Derived helpers ───────────────────────────────────────────────
    @property
    def bars_dir(self) -> Path:
        return Path(self.data_dir) / "bars" / "day"

    @property
    def metadata_dir(self) -> Path:
        return Path(self.data_dir) / "metadata"

    @property
    def intraday_dir(self) -> Path:
        return Path(self.data_dir) / "bars" / "minute"

    @property
    def options_intraday_dir(self) -> Path:
        return Path(self.data_dir) / "options" / "snapshots_intraday"

    @property
    def options_dir(self) -> Path:
        return Path(self.data_dir) / "options" / "snapshots"

    @property
    def resolved_db_path(self) -> str:
        if self.db_path:
            return self.db_path
        return str(Path(self.data_dir) / "deltastack.duckdb")

    @property
    def resolved_backup_dir(self) -> Path:
        if self.backup_dir:
            return Path(self.backup_dir)
        return Path(self.data_dir) / "backups"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton of the settings object."""
    return Settings()
