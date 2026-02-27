"""DeltaStack middleware: API-key authentication, per-IP rate limiting, and
request counting for observability.

Auth
----
* ``X-API-Key`` header is checked against ``DELTASTACK_API_KEY`` env var.
* Exempt paths: ``/health``, ``/docs``, ``/redoc``, ``/openapi.json``, ``/metrics/basic``.
* If ``DELTASTACK_API_KEY`` is **not configured** (empty), all non-exempt
  endpoints return **503 Service Unavailable** with an instructive message.

Rate Limiting
-------------
* In-memory token-bucket per client IP for write endpoints.
* Respects ``X-Real-IP`` / ``X-Forwarded-For`` (set by Nginx).
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from threading import Lock
from typing import Dict, Tuple

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from deltastack.config import get_settings

logger = logging.getLogger(__name__)

# ── paths that never require auth ────────────────────────────────────────────
_PUBLIC_PATHS = frozenset({
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/metrics/basic",
})

# ── rate-limited path prefixes mapped to config attribute names ──────────────
_RATE_LIMITED: Dict[str, str] = {
    "/ingest/": "ingest_rpm",
    "/backtest/": "backtest_rpm",
    "/execute/": "trade_rpm",
    "/trade/": "trade_rpm",
    "/options/chain/snapshot": "options_rpm",
    "/options/greeks": "options_rpm",
}


# ═══════════════════════════════════════════════════════════════════════════════
# API-Key Authentication
# ═══════════════════════════════════════════════════════════════════════════════

class APIKeyMiddleware(BaseHTTPMiddleware):
    """Enforce ``X-API-Key`` header on non-public endpoints."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path

        # Allow public / doc paths through unconditionally
        if path in _PUBLIC_PATHS or path.startswith("/docs") or path.startswith("/redoc"):
            return await call_next(request)

        settings = get_settings()
        expected_key = settings.deltastack_api_key

        # Fail-safe: if key is NOT configured, block everything except public
        if not expected_key:
            logger.warning("DELTASTACK_API_KEY not set – returning 503 for %s", path)
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "Service not fully configured. Set DELTASTACK_API_KEY in .env and restart.",
                },
            )

        provided_key = request.headers.get("X-API-Key", "")
        if provided_key != expected_key:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or missing X-API-Key header."},
            )

        return await call_next(request)


# ═══════════════════════════════════════════════════════════════════════════════
# In-memory Token-Bucket Rate Limiter
# ═══════════════════════════════════════════════════════════════════════════════

class _TokenBucket:
    """Simple token-bucket implementation (not distributed – single process)."""

    def __init__(self, rate_per_minute: int) -> None:
        self.rate = rate_per_minute / 60.0  # tokens per second
        self.max_tokens = float(rate_per_minute)
        self._buckets: Dict[str, Tuple[float, float]] = {}  # ip -> (tokens, last_ts)
        self._lock = Lock()

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        with self._lock:
            tokens, last = self._buckets.get(key, (self.max_tokens, now))
            elapsed = now - last
            tokens = min(self.max_tokens, tokens + elapsed * self.rate)
            if tokens >= 1.0:
                self._buckets[key] = (tokens - 1.0, now)
                return True
            self._buckets[key] = (tokens, now)
            return False


# One bucket per rate-limit category
_buckets: Dict[str, _TokenBucket] = {}


def _get_bucket(config_attr: str) -> _TokenBucket:
    if config_attr not in _buckets:
        settings = get_settings()
        rpm = getattr(settings, config_attr, 60)
        _buckets[config_attr] = _TokenBucket(rpm)
    return _buckets[config_attr]


def _client_ip(request: Request) -> str:
    """Extract real client IP, respecting Nginx proxy headers."""
    forwarded = request.headers.get("X-Real-IP") or request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Enforce per-IP rate limits on write-heavy endpoints."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path
        if request.method == "POST":
            for prefix, config_attr in _RATE_LIMITED.items():
                if path.startswith(prefix):
                    bucket = _get_bucket(config_attr)
                    ip = _client_ip(request)
                    if not bucket.allow(ip):
                        logger.warning("Rate limit exceeded for %s on %s", ip, path)
                        return JSONResponse(
                            status_code=429,
                            content={"detail": "Rate limit exceeded. Try again shortly."},
                        )
                    break

        return await call_next(request)


# ═══════════════════════════════════════════════════════════════════════════════
# Request-counting middleware (lightweight observability)
# ═══════════════════════════════════════════════════════════════════════════════

class ObservabilityMiddleware(BaseHTTPMiddleware):
    """Increment in-memory counters for observability."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Lazy import to avoid circular
        from api.routers.metrics import increment, set_timestamp

        path = request.url.path
        increment("requests_total")

        if path.startswith("/ingest"):
            increment("ingest_requests")
            set_timestamp("last_ingest_time")
        elif path.startswith("/backtest"):
            increment("backtest_requests")
            set_timestamp("last_backtest_time")
        elif path.startswith("/trade"):
            increment("trade_requests")

        return await call_next(request)
