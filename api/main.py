"""DeltaStack FastAPI application.

Entry-point for uvicorn:  ``uvicorn api.main:app --host 127.0.0.1 --port 8000``
"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI

from deltastack.config import get_settings

# ── bootstrap logging ────────────────────────────────────────────────────────
settings = get_settings()
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("deltastack")


# ── lifespan: DB + seed on startup ──────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    from deltastack.db import ensure_tables
    ensure_tables()
    # Seed default agent
    from deltastack.db.dao_agents import seed_mad_max
    seed_mad_max()
    logger.info("DeltaStack API v1.1.0 ready – broker=%s mode=%s",
                settings.broker_provider, settings.broker_mode)
    yield


# ── app ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="DeltaStack",
    description="Agent platform: market data, backtesting, options strategies, paper trading & orchestration",
    version="1.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


# ── middleware ───────────────────────────────────────────────────────────────
from api.middleware import APIKeyMiddleware, RateLimitMiddleware, ObservabilityMiddleware  # noqa: E402

app.add_middleware(ObservabilityMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(APIKeyMiddleware)


# ── health ───────────────────────────────────────────────────────────────────
@app.get("/health", tags=["ops"])
def health():
    return {"status": "ok", "service": "deltastack"}


# ── routers ──────────────────────────────────────────────────────────────────
from api.routers.ingest import router as ingest_router            # noqa: E402
from api.routers.prices import router as prices_router            # noqa: E402
from api.routers.backtest import router as backtest_router        # noqa: E402
from api.routers.data_status import router as data_status_router  # noqa: E402
from api.routers.signals import router as signals_router          # noqa: E402
from api.routers.options import router as options_router          # noqa: E402
from api.routers.trade import router as trade_router              # noqa: E402
from api.routers.metrics import router as metrics_router          # noqa: E402
from api.routers.stats import router as stats_router              # noqa: E402
from api.routers.execute import router as execute_router          # noqa: E402
from api.routers.portfolio import router as portfolio_router      # noqa: E402
from api.routers.ops import router as ops_router                  # noqa: E402
from api.routers.intraday import router as intraday_router        # noqa: E402
from api.routers.orders import router as orders_router            # noqa: E402
from api.routers.freshness import router as freshness_router      # noqa: E402
from api.routers.orchestrate import router as orchestrate_router  # noqa: E402
from api.routers.risk import router as risk_router                # noqa: E402
from api.routers.dashboard import router as dashboard_router      # noqa: E402
from api.routers.agents import router as agents_router            # noqa: E402

app.include_router(ingest_router)
app.include_router(prices_router)
app.include_router(backtest_router)
app.include_router(data_status_router)
app.include_router(signals_router)
app.include_router(options_router)
app.include_router(trade_router)
app.include_router(metrics_router)
app.include_router(stats_router)
app.include_router(execute_router)
app.include_router(portfolio_router)
app.include_router(ops_router)
app.include_router(intraday_router)
app.include_router(orders_router)
app.include_router(freshness_router)
app.include_router(orchestrate_router)
app.include_router(risk_router)
app.include_router(dashboard_router)
app.include_router(agents_router)
