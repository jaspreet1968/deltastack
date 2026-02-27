"""Options chain snapshot ingestion from Polygon.io.

Uses the ``/v3/reference/options/contracts`` and ``/v3/snapshot/options/{underlyingAsset}``
endpoints.  Falls back gracefully if the account lacks options access.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from deltastack.config import get_settings

logger = logging.getLogger(__name__)


# ── public API ───────────────────────────────────────────────────────────────

def fetch_chain_snapshot(
    underlying: str,
    as_of: date,
) -> dict:
    """Download an options chain snapshot from Polygon and store as Parquet.

    Returns summary dict with rows count, path, and any warnings.
    """
    underlying = underlying.upper()
    settings = get_settings()
    api_key = settings.massive_api_key
    if not api_key:
        raise RuntimeError("MASSIVE_API_KEY is not set")

    contracts = _download_snapshot(underlying, as_of, api_key)
    if not contracts:
        return {
            "underlying": underlying,
            "as_of": str(as_of),
            "rows": 0,
            "path": "",
            "warning": "No options data returned from Polygon (may require Options add-on)",
        }

    df = _contracts_to_df(contracts, as_of)
    path = _save_snapshot(underlying, as_of, df)

    return {
        "underlying": underlying,
        "as_of": str(as_of),
        "rows": len(df),
        "path": str(path),
    }


# ── storage ──────────────────────────────────────────────────────────────────

def _snapshot_dir(underlying: str, as_of: date) -> Path:
    settings = get_settings()
    return settings.options_dir / f"underlying={underlying}" / f"as_of={as_of.isoformat()}"


def _save_snapshot(underlying: str, as_of: date, df: pd.DataFrame) -> Path:
    dest = _snapshot_dir(underlying, as_of)
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / "data.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression="snappy")
    logger.info("Saved %d option contracts for %s as_of=%s -> %s", len(df), underlying, as_of, path)
    return path


def load_chain(
    underlying: str,
    as_of: date,
    expiration: Optional[date] = None,
    option_type: Optional[str] = None,
    strike_min: Optional[float] = None,
    strike_max: Optional[float] = None,
) -> pd.DataFrame:
    """Load stored options chain snapshot with optional filters."""
    path = _snapshot_dir(underlying.upper(), as_of) / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(f"No options snapshot for {underlying.upper()} as_of={as_of}")

    df = pd.read_parquet(path)

    if expiration:
        df = df[df["expiration"] == str(expiration)]
    if option_type:
        df = df[df["type"] == option_type.lower()]
    if strike_min is not None:
        df = df[df["strike"] >= strike_min]
    if strike_max is not None:
        df = df[df["strike"] <= strike_max]

    return df


# ── Polygon API ──────────────────────────────────────────────────────────────

def _download_snapshot(underlying: str, as_of: date, api_key: str) -> list:
    """Attempt to fetch options snapshot from Polygon.

    Tries the snapshot endpoint first; if that returns 403/404, falls back
    to the contracts reference endpoint with a warning.
    """
    import requests

    # Try snapshot endpoint (requires Options add-on)
    url = f"https://api.polygon.io/v3/snapshot/options/{underlying}"
    params = {"apiKey": api_key, "limit": 250}

    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            body = resp.json()
            results = body.get("results", [])
            # Paginate
            next_url = body.get("next_url")
            while next_url:
                r2 = requests.get(next_url, params={"apiKey": api_key}, timeout=30)
                if r2.status_code != 200:
                    break
                b2 = r2.json()
                results.extend(b2.get("results", []))
                next_url = b2.get("next_url")
            logger.info("Polygon snapshot returned %d contracts for %s", len(results), underlying)
            return results
        elif resp.status_code in (403, 404):
            logger.warning(
                "Polygon options snapshot returned %d – may require Options add-on. "
                "Falling back to contracts reference.",
                resp.status_code,
            )
            return _download_contracts_reference(underlying, as_of, api_key)
        else:
            resp.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.warning("Snapshot endpoint failed; trying contracts reference")
        return _download_contracts_reference(underlying, as_of, api_key)

    return []


def _download_contracts_reference(underlying: str, as_of: date, api_key: str) -> list:
    """Fallback: use /v3/reference/options/contracts for basic contract info."""
    import requests

    url = "https://api.polygon.io/v3/reference/options/contracts"
    params = {
        "underlying_ticker": underlying,
        "as_of": as_of.isoformat(),
        "limit": 1000,
        "apiKey": api_key,
    }
    all_results = []
    while url:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            logger.warning("Contracts reference returned %d", resp.status_code)
            break
        body = resp.json()
        all_results.extend(body.get("results", []))
        url = body.get("next_url")
        params = {"apiKey": api_key}  # next_url includes other params

    logger.info("Contracts reference returned %d contracts for %s", len(all_results), underlying)
    return all_results


def _contracts_to_df(contracts: list, as_of: date) -> pd.DataFrame:
    """Normalise raw Polygon contract data to a flat DataFrame."""
    rows = []
    for c in contracts:
        # Snapshot format has nested 'details' + 'day' + 'greeks'
        details = c.get("details", c)
        day = c.get("day", {})
        greeks = c.get("greeks", {})

        row = {
            "as_of": str(as_of),
            "ticker": details.get("ticker", c.get("ticker", "")),
            "underlying": details.get("underlying_ticker", c.get("underlying_ticker", "")),
            "type": details.get("contract_type", c.get("contract_type", "")).lower(),
            "strike": details.get("strike_price", c.get("strike_price")),
            "expiration": details.get("expiration_date", c.get("expiration_date", "")),
            "bid": day.get("close", c.get("bid", None)),
            "ask": day.get("high", c.get("ask", None)),
            "last": day.get("last_updated", c.get("last_price", None)),
            "volume": day.get("volume", c.get("volume", 0)),
            "open_interest": c.get("open_interest", 0),
            "iv": greeks.get("implied_volatility", c.get("implied_volatility", None)),
            "delta": greeks.get("delta", None),
            "gamma": greeks.get("gamma", None),
            "theta": greeks.get("theta", None),
            "vega": greeks.get("vega", None),
        }
        rows.append(row)

    return pd.DataFrame(rows) if rows else pd.DataFrame()
