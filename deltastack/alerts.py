"""Outbound alert/notification system – webhook-based.

Sends JSON payloads to ALERT_WEBHOOK_URL if configured.
All secrets are redacted before sending.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

import requests

from deltastack.config import get_settings

logger = logging.getLogger(__name__)


def send_alert(
    *,
    title: str,
    message: str,
    level: str = "INFO",
    context: Optional[dict] = None,
) -> bool:
    """Send an alert to the configured webhook URL.

    Returns True if sent, False if skipped or failed.
    """
    settings = get_settings()

    # Check level threshold
    levels = {"DEBUG": 0, "INFO": 1, "WARN": 2, "ERROR": 3}
    if levels.get(level, 0) < levels.get(settings.alert_level, 1):
        return False

    url = settings.alert_webhook_url
    if not url:
        logger.debug("No ALERT_WEBHOOK_URL configured – skipping alert")
        return False

    payload = {
        "title": title,
        "message": message,
        "level": level,
        "service": "deltastack",
        "context": _redact(context or {}),
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code < 300:
            logger.info("Alert sent: %s (%s)", title, level)
            return True
        logger.warning("Alert webhook returned %d", resp.status_code)
        return False
    except Exception as exc:
        logger.warning("Alert webhook failed: %s", exc)
        return False


def _redact(data: dict) -> dict:
    """Remove sensitive keys from context before sending."""
    sensitive = {"api_key", "secret_key", "token", "password", "access_token"}
    return {
        k: "***REDACTED***" if any(s in k.lower() for s in sensitive) else v
        for k, v in data.items()
    }
