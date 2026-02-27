"""DuckDB connection management and automatic table creation.

Tables are created idempotently (CREATE TABLE IF NOT EXISTS) on every call
to ``ensure_tables()``.  The FastAPI lifespan hook calls this at startup.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path

import duckdb

from deltastack.config import get_settings

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id        VARCHAR PRIMARY KEY,
    created_at    TIMESTAMP DEFAULT current_timestamp,
    strategy      VARCHAR NOT NULL,
    tickers       VARCHAR NOT NULL,
    params_json   VARCHAR DEFAULT '{}',
    dt_start      DATE,
    dt_end        DATE,
    metrics_json  VARCHAR DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS trades (
    trade_id      VARCHAR PRIMARY KEY,
    run_id        VARCHAR,
    ticker        VARCHAR NOT NULL,
    side          VARCHAR NOT NULL,
    qty           DOUBLE DEFAULT 0,
    entry_time    VARCHAR,
    entry_price   DOUBLE,
    exit_time     VARCHAR,
    exit_price    DOUBLE,
    pnl           DOUBLE DEFAULT 0,
    meta_json     VARCHAR DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS positions (
    position_id   INTEGER DEFAULT nextval('seq_position_id'),
    as_of         TIMESTAMP DEFAULT current_timestamp,
    ticker        VARCHAR NOT NULL,
    qty           DOUBLE DEFAULT 0,
    avg_price     DOUBLE DEFAULT 0,
    unrealized_pnl DOUBLE DEFAULT 0,
    meta_json     VARCHAR DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS signals (
    signal_id     INTEGER DEFAULT nextval('seq_signal_id'),
    created_at    TIMESTAMP DEFAULT current_timestamp,
    strategy      VARCHAR NOT NULL,
    ticker        VARCHAR NOT NULL,
    signal        VARCHAR NOT NULL,
    as_of         VARCHAR,
    meta_json     VARCHAR DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id        VARCHAR PRIMARY KEY,
    started_at    TIMESTAMP DEFAULT current_timestamp,
    ended_at      TIMESTAMP,
    tickers       VARCHAR NOT NULL,
    status        VARCHAR DEFAULT 'running',
    rows_total    INTEGER DEFAULT 0,
    error_summary VARCHAR DEFAULT ''
);

CREATE TABLE IF NOT EXISTS order_requests (
    request_id    INTEGER DEFAULT nextval('seq_order_request_id'),
    created_at    TIMESTAMP DEFAULT current_timestamp,
    client_ip     VARCHAR DEFAULT '',
    ticker        VARCHAR NOT NULL,
    side          VARCHAR NOT NULL,
    qty           DOUBLE DEFAULT 0,
    requested_price DOUBLE DEFAULT 0,
    accepted      BOOLEAN DEFAULT false,
    reject_reason VARCHAR DEFAULT ''
);

CREATE TABLE IF NOT EXISTS options_backtest_runs (
    run_id        VARCHAR PRIMARY KEY,
    created_at    TIMESTAMP DEFAULT current_timestamp,
    strategy      VARCHAR NOT NULL,
    underlying    VARCHAR NOT NULL,
    params_json   VARCHAR DEFAULT '{}',
    metrics_json  VARCHAR DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS options_trades (
    trade_id      VARCHAR PRIMARY KEY,
    run_id        VARCHAR,
    underlying    VARCHAR NOT NULL,
    strategy      VARCHAR NOT NULL,
    short_strike  DOUBLE,
    long_strike   DOUBLE,
    expiration    VARCHAR,
    option_type   VARCHAR,
    contracts     INTEGER DEFAULT 1,
    credit        DOUBLE DEFAULT 0,
    max_loss      DOUBLE DEFAULT 0,
    pnl           DOUBLE DEFAULT 0,
    exit_reason   VARCHAR DEFAULT '',
    meta_json     VARCHAR DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS execution_plans (
    plan_id       VARCHAR PRIMARY KEY,
    created_at    TIMESTAMP DEFAULT current_timestamp,
    request_json  VARCHAR DEFAULT '{}',
    orders_json   VARCHAR DEFAULT '[]',
    risk_summary  VARCHAR DEFAULT '{}',
    status        VARCHAR DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS execution_events (
    event_id      INTEGER DEFAULT nextval('seq_exec_event_id'),
    plan_id       VARCHAR NOT NULL,
    event_time    TIMESTAMP DEFAULT current_timestamp,
    event_type    VARCHAR NOT NULL,
    details_json  VARCHAR DEFAULT '{}'
);
"""

_SEQ_DDL = """
CREATE SEQUENCE IF NOT EXISTS seq_position_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_signal_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_order_request_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_exec_event_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_health_check_id START 1;
"""

_DDL_PHASE_F = """
CREATE TABLE IF NOT EXISTS health_checks (
    check_id      INTEGER DEFAULT nextval('seq_health_check_id'),
    checked_at    TIMESTAMP DEFAULT current_timestamp,
    status        VARCHAR DEFAULT 'ok',
    details_json  VARCHAR DEFAULT '{}'
);
"""

_DDL_PHASE_G = """
CREATE TABLE IF NOT EXISTS orders (
    order_id        VARCHAR PRIMARY KEY,
    provider        VARCHAR DEFAULT 'paper',
    status          VARCHAR DEFAULT 'CREATED',
    created_at      TIMESTAMP DEFAULT current_timestamp,
    updated_at      TIMESTAMP DEFAULT current_timestamp,
    request_json    VARCHAR DEFAULT '{}',
    response_json   VARCHAR DEFAULT '{}',
    filled_qty      DOUBLE DEFAULT 0,
    avg_fill_price  DOUBLE DEFAULT 0,
    idempotency_key VARCHAR DEFAULT ''
);

CREATE TABLE IF NOT EXISTS errors (
    error_id      INTEGER DEFAULT nextval('seq_error_id'),
    created_at    TIMESTAMP DEFAULT current_timestamp,
    component     VARCHAR NOT NULL,
    severity      VARCHAR DEFAULT 'error',
    message       VARCHAR DEFAULT '',
    context_json  VARCHAR DEFAULT '{}'
);
"""

_SEQ_PHASE_G = """
CREATE SEQUENCE IF NOT EXISTS seq_error_id START 1;
"""

_DDL_PHASE_H = """
CREATE TABLE IF NOT EXISTS orchestration_runs (
    batch_id      VARCHAR PRIMARY KEY,
    created_at    TIMESTAMP DEFAULT current_timestamp,
    run_date      DATE,
    mode          VARCHAR DEFAULT 'dry_run',
    status        VARCHAR DEFAULT 'running',
    summary_json  VARCHAR DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS walk_forward_runs (
    run_id        VARCHAR PRIMARY KEY,
    created_at    TIMESTAMP DEFAULT current_timestamp,
    ticker        VARCHAR NOT NULL,
    dt_start      DATE,
    dt_end        DATE,
    params_json   VARCHAR DEFAULT '{}',
    metrics_json  VARCHAR DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS walk_forward_folds (
    fold_id       INTEGER DEFAULT nextval('seq_wf_fold_id'),
    run_id        VARCHAR NOT NULL,
    fold_num      INTEGER DEFAULT 0,
    train_start   DATE,
    train_end     DATE,
    test_start    DATE,
    test_end      DATE,
    chosen_params VARCHAR DEFAULT '{}',
    train_metric  DOUBLE DEFAULT 0,
    test_metric   DOUBLE DEFAULT 0
);
"""

_SEQ_PHASE_H = """
CREATE SEQUENCE IF NOT EXISTS seq_wf_fold_id START 1;
"""

_DDL_AGENTS = """
CREATE TABLE IF NOT EXISTS agents (
    agent_id      VARCHAR PRIMARY KEY,
    name          VARCHAR UNIQUE NOT NULL,
    display_name  VARCHAR DEFAULT '',
    description   VARCHAR DEFAULT '',
    risk_profile  VARCHAR DEFAULT 'BALANCED',
    broker_provider VARCHAR DEFAULT 'paper',
    enabled       BOOLEAN DEFAULT true,
    created_at    TIMESTAMP DEFAULT current_timestamp,
    updated_at    TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS agent_strategies (
    agent_strategy_id VARCHAR PRIMARY KEY,
    agent_id          VARCHAR NOT NULL,
    strategy_name     VARCHAR NOT NULL,
    params_json       VARCHAR DEFAULT '{}',
    schedule_json     VARCHAR DEFAULT '{}',
    execution_mode    VARCHAR DEFAULT 'plan_only',
    enabled           BOOLEAN DEFAULT true,
    created_at        TIMESTAMP DEFAULT current_timestamp,
    updated_at        TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS agent_runs (
    run_id              VARCHAR PRIMARY KEY,
    agent_id            VARCHAR NOT NULL,
    agent_strategy_id   VARCHAR DEFAULT '',
    run_type            VARCHAR DEFAULT 'signal',
    status              VARCHAR DEFAULT 'running',
    started_at          TIMESTAMP DEFAULT current_timestamp,
    ended_at            TIMESTAMP,
    summary_json        VARCHAR DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS agent_pnl_daily (
    agent_id      VARCHAR NOT NULL,
    pnl_date      DATE NOT NULL,
    equity        DOUBLE DEFAULT 0,
    pnl_day       DOUBLE DEFAULT 0,
    pnl_total     DOUBLE DEFAULT 0,
    drawdown      DOUBLE DEFAULT 0,
    exposure_json VARCHAR DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS run_agent_map (
    run_id              VARCHAR NOT NULL,
    agent_id            VARCHAR NOT NULL,
    agent_strategy_id   VARCHAR DEFAULT ''
);

CREATE TABLE IF NOT EXISTS options_intraday_index (
    idx_id        INTEGER DEFAULT nextval('seq_oi_idx_id'),
    underlying    VARCHAR NOT NULL,
    snap_date     DATE NOT NULL,
    snap_time     VARCHAR NOT NULL,
    rows_count    INTEGER DEFAULT 0,
    expirations   VARCHAR DEFAULT '',
    last_updated  TIMESTAMP DEFAULT current_timestamp
);
"""

_SEQ_AGENTS = """
CREATE SEQUENCE IF NOT EXISTS seq_oi_idx_id START 1;
"""

_DDL_PHASE_I = """
CREATE TABLE IF NOT EXISTS options_snapshot_runs (
    snap_run_id   INTEGER DEFAULT nextval('seq_snap_run_id'),
    underlying    VARCHAR NOT NULL,
    snap_date     DATE NOT NULL,
    snap_time     VARCHAR NOT NULL,
    status        VARCHAR DEFAULT 'ok',
    rows_count    INTEGER DEFAULT 0,
    error_msg     VARCHAR DEFAULT ''
);

CREATE TABLE IF NOT EXISTS agent_replays (
    replay_id     VARCHAR PRIMARY KEY,
    agent_id      VARCHAR NOT NULL,
    replay_date   DATE NOT NULL,
    params_json   VARCHAR DEFAULT '{}',
    created_at    TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS agent_replay_ticks (
    tick_id       INTEGER DEFAULT nextval('seq_replay_tick_id'),
    replay_id     VARCHAR NOT NULL,
    tick_time     VARCHAR NOT NULL,
    signal_json   VARCHAR DEFAULT '{}',
    decision_json VARCHAR DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS strategy_status_events (
    event_id      INTEGER DEFAULT nextval('seq_strat_status_id'),
    created_at    TIMESTAMP DEFAULT current_timestamp,
    agent_id      VARCHAR NOT NULL,
    agent_strategy_id VARCHAR NOT NULL,
    old_status    VARCHAR DEFAULT '',
    new_status    VARCHAR DEFAULT '',
    reason        VARCHAR DEFAULT ''
);
"""

_SEQ_PHASE_I = """
CREATE SEQUENCE IF NOT EXISTS seq_snap_run_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_replay_tick_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_strat_status_id START 1;
"""


@lru_cache(maxsize=1)
def get_db() -> duckdb.DuckDBPyConnection:
    """Return a singleton DuckDB connection (thread-safe in DuckDB >= 0.9)."""
    settings = get_settings()
    db_path = settings.resolved_db_path
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    logger.info("Opening DuckDB at %s", db_path)
    conn = duckdb.connect(db_path)
    return conn


def ensure_tables() -> None:
    """Create all required tables if they don't exist."""
    conn = get_db()
    conn.execute(_SEQ_DDL)
    conn.execute(_DDL)
    conn.execute(_DDL_PHASE_F)
    conn.execute(_SEQ_PHASE_G)
    conn.execute(_DDL_PHASE_G)
    conn.execute(_SEQ_PHASE_H)
    conn.execute(_DDL_PHASE_H)
    conn.execute(_DDL_AGENTS)
    conn.execute(_SEQ_AGENTS)
    conn.execute(_SEQ_PHASE_I)
    conn.execute(_DDL_PHASE_I)
    logger.info("DuckDB tables ensured")
