"""Lightweight Black-Scholes greeks calculator.

No external dependencies beyond numpy/scipy.  All functions are pure and
stateless – safe to call from any thread.

Notation
--------
S   spot price
K   strike
T   time to expiry in years
r   risk-free rate (annualised, continuous)
sigma (iv)  implied volatility (annualised)
"""

from __future__ import annotations

import logging
import math
from typing import Optional

import numpy as np
from scipy.stats import norm

logger = logging.getLogger(__name__)


# ── Black-Scholes pricing ───────────────────────────────────────────────────

def _d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
    return (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))


def _d2(S: float, K: float, T: float, r: float, sigma: float) -> float:
    return _d1(S, K, T, r, sigma) - sigma * math.sqrt(T)


def bs_call_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
    d1 = _d1(S, K, T, r, sigma)
    d2 = _d2(S, K, T, r, sigma)
    return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)


def bs_put_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
    d1 = _d1(S, K, T, r, sigma)
    d2 = _d2(S, K, T, r, sigma)
    return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


# ── Greeks ───────────────────────────────────────────────────────────────────

def compute_greeks(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    option_type: str = "call",
) -> dict:
    """Compute delta, gamma, theta, vega for a European option.

    Returns a dict with keys: delta, gamma, theta, vega, price.
    All values are per-share (not per-contract).
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return {"delta": 0, "gamma": 0, "theta": 0, "vega": 0, "price": 0, "error": "invalid_inputs"}

    d1 = _d1(S, K, T, r, sigma)
    d2 = _d2(S, K, T, r, sigma)
    sqrt_T = math.sqrt(T)
    pdf_d1 = norm.pdf(d1)

    # Gamma (same for call and put)
    gamma = pdf_d1 / (S * sigma * sqrt_T)

    # Vega (same for call and put) – per 1% move in vol
    vega = S * pdf_d1 * sqrt_T / 100.0

    if option_type.lower() == "call":
        delta = norm.cdf(d1)
        theta = (
            -(S * pdf_d1 * sigma) / (2 * sqrt_T)
            - r * K * math.exp(-r * T) * norm.cdf(d2)
        ) / 365.0  # per calendar day
        price = bs_call_price(S, K, T, r, sigma)
    else:
        delta = norm.cdf(d1) - 1.0
        theta = (
            -(S * pdf_d1 * sigma) / (2 * sqrt_T)
            + r * K * math.exp(-r * T) * norm.cdf(-d2)
        ) / 365.0
        price = bs_put_price(S, K, T, r, sigma)

    return {
        "delta": round(delta, 6),
        "gamma": round(gamma, 6),
        "theta": round(theta, 6),
        "vega": round(vega, 6),
        "price": round(price, 4),
    }


# ── Implied Volatility (Newton-Raphson) ─────────────────────────────────────

def implied_vol(
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    option_type: str = "call",
    tol: float = 1e-6,
    max_iter: int = 100,
) -> Optional[float]:
    """Solve for implied volatility using Newton-Raphson.

    Returns None if the solver fails to converge.
    """
    if market_price <= 0 or T <= 0 or S <= 0 or K <= 0:
        return None

    sigma = 0.3  # initial guess
    for _ in range(max_iter):
        if option_type.lower() == "call":
            price = bs_call_price(S, K, T, r, sigma)
        else:
            price = bs_put_price(S, K, T, r, sigma)

        diff = price - market_price

        # Vega for Newton step
        d1 = _d1(S, K, T, r, sigma)
        vega_raw = S * norm.pdf(d1) * math.sqrt(T)
        if abs(vega_raw) < 1e-12:
            return None

        sigma -= diff / vega_raw

        if sigma <= 0:
            sigma = 0.001

        if abs(diff) < tol:
            return round(sigma, 6)

    logger.debug("IV solver did not converge for S=%.2f K=%.2f T=%.4f", S, K, T)
    return None
