"""In-memory TTL LRU cache for read-heavy data paths.

Wraps ``load_bars`` and similar functions so repeated API requests within the
TTL window are served from memory instead of reading Parquet from disk.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import OrderedDict
from datetime import date
from threading import Lock
from typing import Any, Optional

import pandas as pd

from deltastack.config import get_settings

logger = logging.getLogger(__name__)


class TTLCache:
    """Thread-safe LRU cache with per-entry TTL."""

    def __init__(self, max_size: int = 256, ttl: int = 60) -> None:
        self._cache: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._lock = Lock()
        self.max_size = max_size
        self.ttl = ttl
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._cache:
                ts, val = self._cache[key]
                if time.monotonic() - ts < self.ttl:
                    self._cache.move_to_end(key)
                    self.hits += 1
                    return val
                else:
                    del self._cache[key]
            self.misses += 1
            return None

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
            elif len(self._cache) >= self.max_size:
                self._cache.popitem(last=False)
            self._cache[key] = (time.monotonic(), value)

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._cache.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    @property
    def size(self) -> int:
        return len(self._cache)

    def stats(self) -> dict:
        return {
            "size": self.size,
            "max_size": self.max_size,
            "ttl_seconds": self.ttl,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": round(self.hits / max(self.hits + self.misses, 1), 4),
        }


# ── singleton caches ─────────────────────────────────────────────────────────
_bars_cache: Optional[TTLCache] = None
_options_cache: Optional[TTLCache] = None


def get_bars_cache() -> TTLCache:
    global _bars_cache
    if _bars_cache is None:
        s = get_settings()
        _bars_cache = TTLCache(max_size=s.cache_max_size, ttl=s.cache_ttl_seconds)
    return _bars_cache


def get_options_cache() -> TTLCache:
    global _options_cache
    if _options_cache is None:
        s = get_settings()
        _options_cache = TTLCache(max_size=s.cache_max_size, ttl=s.cache_ttl_seconds)
    return _options_cache


def make_cache_key(*parts: Any) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()
