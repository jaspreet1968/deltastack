"""Broker factory – selects and validates broker at runtime.

Safety rules:
- BROKER_MODE must be "paper" – any other value is blocked.
- Secrets must not be placeholder values.
- Broker base URLs must be sandbox/paper/SIM endpoints.

Supported providers: paper, alpaca, tradier, tradestation
"""

from __future__ import annotations

import logging
from typing import Optional

from deltastack.broker.base import Broker
from deltastack.config import get_settings

logger = logging.getLogger(__name__)

_PLACEHOLDER_VALUES = frozenset({
    "your_polygon_api_key_here",
    "your_secure_api_key_here",
    "YOUR_KEY",
    "changeme",
    "placeholder",
    "",
})

_broker_instance: Optional[Broker] = None
_broker_error: str = ""


def _validate_secrets() -> Optional[str]:
    """Return error message if secrets are invalid, else None."""
    settings = get_settings()
    if settings.broker_mode != "paper":
        return f"BROKER_MODE='{settings.broker_mode}' is not allowed. Must be 'paper'."

    provider = settings.broker_provider.lower()

    if provider == "alpaca":
        if settings.alpaca_api_key.lower() in _PLACEHOLDER_VALUES:
            return "ALPACA_API_KEY is missing or set to a placeholder."
        if settings.alpaca_secret_key.lower() in _PLACEHOLDER_VALUES:
            return "ALPACA_SECRET_KEY is missing or set to a placeholder."

    elif provider == "tradier":
        if settings.tradier_access_token.lower() in _PLACEHOLDER_VALUES:
            return "TRADIER_ACCESS_TOKEN is missing or set to a placeholder."

    elif provider == "tradestation":
        if settings.tradestation_client_id.lower() in _PLACEHOLDER_VALUES:
            return "TRADESTATION_CLIENT_ID is missing or set to a placeholder."
        if settings.tradestation_client_secret.lower() in _PLACEHOLDER_VALUES:
            return "TRADESTATION_CLIENT_SECRET is missing or set to a placeholder."

    return None


def get_broker() -> Broker:
    """Return the configured broker instance (cached singleton)."""
    global _broker_instance, _broker_error

    if _broker_instance is not None:
        return _broker_instance

    settings = get_settings()

    # Validate mode
    err = _validate_secrets()
    if err:
        _broker_error = err
        raise RuntimeError(f"Broker configuration error: {err}")

    provider = settings.broker_provider.lower()

    if provider == "paper":
        from deltastack.broker.paper import PaperBroker
        _broker_instance = PaperBroker()
        logger.info("Broker: PaperBroker (simulated)")

    elif provider == "alpaca":
        from deltastack.broker.alpaca import AlpacaBroker
        _broker_instance = AlpacaBroker()
        logger.info("Broker: Alpaca paper (%s)", settings.alpaca_base_url)

    elif provider == "tradier":
        from deltastack.broker.tradier import TradierBroker
        _broker_instance = TradierBroker()
        logger.info("Broker: Tradier sandbox (%s)", settings.tradier_base_url)

    elif provider == "tradestation":
        from deltastack.broker.tradestation import TradeStationBroker
        _broker_instance = TradeStationBroker()
        logger.info("Broker: TradeStation SIM (%s)", settings.tradestation_base_url)

    else:
        _broker_error = f"Unknown BROKER_PROVIDER: '{provider}'. Supported: paper, alpaca, tradier, tradestation"
        raise RuntimeError(_broker_error)

    _broker_error = ""
    return _broker_instance


def get_broker_status() -> dict:
    """Return broker status without raising."""
    global _broker_error
    settings = get_settings()

    status = {
        "provider": settings.broker_provider,
        "mode": settings.broker_mode,
        "trading_enabled": settings.trading_enabled,
    }

    err = _validate_secrets()
    if err:
        status["ok"] = False
        status["error"] = err
        return status

    provider = settings.broker_provider.lower()

    if provider == "alpaca":
        from deltastack.broker.alpaca import _is_paper_url
        status["base_url"] = settings.alpaca_base_url
        status["paper_url_ok"] = _is_paper_url(settings.alpaca_base_url)
        if not status["paper_url_ok"]:
            status["ok"] = False
            status["error"] = "Alpaca base URL is not a paper endpoint"
            return status

    elif provider == "tradier":
        from deltastack.broker.tradier import _is_sandbox_url
        status["base_url"] = settings.tradier_base_url
        status["paper_url_ok"] = _is_sandbox_url(settings.tradier_base_url)
        if not status["paper_url_ok"]:
            status["ok"] = False
            status["error"] = "Tradier base URL is not a sandbox endpoint"
            return status

    elif provider == "tradestation":
        from deltastack.broker.tradestation import _is_sim_url
        status["base_url"] = settings.tradestation_base_url
        status["paper_url_ok"] = _is_sim_url(settings.tradestation_base_url)
        if not status["paper_url_ok"]:
            status["ok"] = False
            status["error"] = "TradeStation base URL is not a SIM endpoint"
            return status

    status["ok"] = True
    status["last_broker_error"] = _broker_error
    return status


def reset_broker() -> None:
    """Reset singleton (for testing)."""
    global _broker_instance, _broker_error
    _broker_instance = None
    _broker_error = ""
