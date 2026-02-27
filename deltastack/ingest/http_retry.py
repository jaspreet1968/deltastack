"""Robust HTTP GET with exponential backoff and 429 handling.

Used by Polygon ingestion modules to survive transient failures and
rate limits without crashing the entire ingestion pipeline.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import requests

from deltastack.config import get_settings

logger = logging.getLogger(__name__)


def get_with_retry(
    url: str,
    params: Optional[dict] = None,
    *,
    max_retries: Optional[int] = None,
    backoff_base: Optional[float] = None,
    timeout: Optional[int] = None,
) -> requests.Response:
    """HTTP GET with configurable exponential backoff.

    Handles:
    * Connection errors, timeouts → retry
    * 429 Too Many Requests → respect Retry-After header, then retry
    * 5xx server errors → retry

    Raises ``requests.exceptions.HTTPError`` after all retries exhausted.
    """
    settings = get_settings()
    retries = max_retries if max_retries is not None else settings.http_max_retries
    base = backoff_base if backoff_base is not None else settings.http_backoff_base
    tout = timeout if timeout is not None else settings.http_timeout

    last_exc: Optional[Exception] = None

    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=tout)

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else base * (2 ** attempt)
                logger.warning(
                    "429 rate limited on %s – waiting %.1fs (attempt %d/%d)",
                    url, wait, attempt + 1, retries + 1,
                )
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = base * (2 ** attempt)
                logger.warning(
                    "Server error %d on %s – retrying in %.1fs (attempt %d/%d)",
                    resp.status_code, url, wait, attempt + 1, retries + 1,
                )
                time.sleep(wait)
                continue

            # Success or client error (4xx except 429) – return immediately
            return resp

        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            wait = base * (2 ** attempt)
            logger.warning(
                "Connection error on %s: %s – retrying in %.1fs (attempt %d/%d)",
                url, exc, wait, attempt + 1, retries + 1,
            )
            time.sleep(wait)

    # All retries exhausted
    if last_exc:
        raise last_exc
    # Return last response even if it was an error
    return resp  # type: ignore[possibly-undefined]
