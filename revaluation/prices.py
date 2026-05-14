"""
Yahoo Finance spot price fetcher for Au, Cu, and AUD/USD FX.

Uses the unofficial query2.finance.yahoo.com endpoint via requests.
No yfinance dependency — that library is heavyweight and we only need quotes.
"""
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Symbol -> (commodity_code, unit, multiplier_to_canonical_unit)
SYMBOL_MAP = {
    "GC=F":     ("Au", "USD/oz", Decimal("1")),
    "HG=F":     ("Cu", "USD/lb", Decimal("1")),
    "AUDUSD=X": ("AUDUSD", "AUD/USD", Decimal("1")),
}

CACHE_TTL_HOURS = 1


class PriceFetchError(Exception):
    pass


def fetch_yahoo_quote(symbol: str) -> Decimal:
    """Single quote lookup. Raises on any failure — no silent fallback."""
    url = "https://query2.finance.yahoo.com/v7/finance/quote"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Quantyc/1.0)",
        "Accept": "application/json",
    }
    params = {"symbols": symbol}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        raise PriceFetchError(f"yahoo_http_error:{type(e).__name__}:{e}")

    results = data.get("quoteResponse", {}).get("result", [])
    if not results:
        raise PriceFetchError(f"yahoo_empty_result:{symbol}")

    price = results[0].get("regularMarketPrice")
    if price is None:
        raise PriceFetchError(f"yahoo_no_price:{symbol}")

    return Decimal(str(price))


def get_or_fetch_price(conn: sqlite3.Connection, commodity: str) -> tuple[Decimal, int]:
    """
    Returns (price, price_id). Uses cache if a price was fetched within CACHE_TTL_HOURS.
    Otherwise fetches fresh from Yahoo and inserts into commodity_prices.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=CACHE_TTL_HOURS)).isoformat()
    cached = conn.execute(
        """SELECT price_id, price_usd FROM commodity_prices
           WHERE commodity = ? AND fetched_at >= ?
           ORDER BY fetched_at DESC LIMIT 1""",
        (commodity, cutoff),
    ).fetchone()

    if cached:
        return Decimal(str(cached[1])), cached[0]

    # Resolve commodity -> symbol
    symbol = None
    unit = None
    for sym, (com, u, _) in SYMBOL_MAP.items():
        if com == commodity:
            symbol = sym
            unit = u
            break
    if symbol is None:
        raise PriceFetchError(f"no_yahoo_symbol_for_commodity:{commodity}")

    price = fetch_yahoo_quote(symbol)
    source = f"yahoo:{symbol}"

    cur = conn.execute(
        """INSERT INTO commodity_prices (commodity, price_usd, unit, source, fetched_at)
           VALUES (?, ?, ?, ?, ?)""",
        (commodity, float(price), unit, source, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    return price, cur.lastrowid
