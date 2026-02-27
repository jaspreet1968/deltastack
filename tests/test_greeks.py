"""Unit tests for deltastack/options/greeks.py."""

import math
import pytest
from deltastack.options.greeks import compute_greeks, bs_call_price, bs_put_price, implied_vol


class TestComputeGreeks:
    """Verify Black-Scholes greeks are within expected ranges."""

    def test_call_delta_range(self):
        g = compute_greeks(S=100, K=100, T=0.25, r=0.05, sigma=0.2, option_type="call")
        assert 0 < g["delta"] < 1, f"Call delta {g['delta']} not in (0,1)"

    def test_put_delta_range(self):
        g = compute_greeks(S=100, K=100, T=0.25, r=0.05, sigma=0.2, option_type="put")
        assert -1 < g["delta"] < 0, f"Put delta {g['delta']} not in (-1,0)"

    def test_gamma_positive(self):
        g = compute_greeks(S=100, K=100, T=0.25, r=0.05, sigma=0.2, option_type="call")
        assert g["gamma"] > 0

    def test_vega_positive(self):
        g = compute_greeks(S=100, K=100, T=0.25, r=0.05, sigma=0.2, option_type="call")
        assert g["vega"] > 0

    def test_theta_negative_for_call(self):
        g = compute_greeks(S=100, K=100, T=0.25, r=0.05, sigma=0.2, option_type="call")
        assert g["theta"] < 0, "ATM call theta should be negative"

    def test_deep_itm_call_delta_near_one(self):
        g = compute_greeks(S=200, K=100, T=0.25, r=0.05, sigma=0.2, option_type="call")
        assert g["delta"] > 0.95

    def test_deep_otm_call_delta_near_zero(self):
        g = compute_greeks(S=50, K=100, T=0.25, r=0.05, sigma=0.2, option_type="call")
        assert g["delta"] < 0.05

    def test_invalid_inputs(self):
        g = compute_greeks(S=0, K=100, T=0.25, r=0.05, sigma=0.2)
        assert g.get("error") == "invalid_inputs"


class TestPutCallParity:
    """Verify put-call parity: C - P = S - K*exp(-rT)."""

    @pytest.mark.parametrize("S,K,T,r,sigma", [
        (100, 100, 0.25, 0.05, 0.2),
        (110, 95, 0.5, 0.03, 0.3),
        (90, 110, 1.0, 0.07, 0.15),
    ])
    def test_parity(self, S, K, T, r, sigma):
        c = bs_call_price(S, K, T, r, sigma)
        p = bs_put_price(S, K, T, r, sigma)
        parity_rhs = S - K * math.exp(-r * T)
        assert abs((c - p) - parity_rhs) < 1e-6, (
            f"Put-call parity violated: C-P={c-p:.6f}, S-Ke^-rT={parity_rhs:.6f}"
        )


class TestImpliedVol:
    """Verify IV solver recovers the original volatility."""

    @pytest.mark.parametrize("sigma", [0.15, 0.25, 0.40])
    def test_roundtrip_call(self, sigma):
        S, K, T, r = 100, 100, 0.25, 0.05
        price = bs_call_price(S, K, T, r, sigma)
        recovered = implied_vol(price, S, K, T, r, option_type="call")
        assert recovered is not None
        assert abs(recovered - sigma) < 0.001, f"Expected ~{sigma}, got {recovered}"

    @pytest.mark.parametrize("sigma", [0.15, 0.25, 0.40])
    def test_roundtrip_put(self, sigma):
        S, K, T, r = 100, 100, 0.25, 0.05
        price = bs_put_price(S, K, T, r, sigma)
        recovered = implied_vol(price, S, K, T, r, option_type="put")
        assert recovered is not None
        assert abs(recovered - sigma) < 0.001

    def test_zero_price_returns_none(self):
        assert implied_vol(0, 100, 100, 0.25, 0.05) is None
