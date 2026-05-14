"""Tests for revaluation price fetcher — mocked HTTP."""
import sqlite3
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch, MagicMock

import pytest

from revaluation.prices import (
    fetch_yahoo_quote,
    get_or_fetch_price,
    PriceFetchError,
    CACHE_TTL_HOURS,
)


@pytest.fixture
def in_memory_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE commodity_prices (
            price_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity  TEXT NOT NULL,
            price_usd  REAL NOT NULL,
            unit       TEXT NOT NULL,
            source     TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


# ── fetch_yahoo_quote ─────────────────────────────────────────────


@patch("revaluation.prices.requests.get")
def test_fetch_yahoo_quote_gold(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "quoteResponse": {"result": [{"symbol": "GC=F", "regularMarketPrice": 3520.5}]}
    }
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    result = fetch_yahoo_quote("GC=F")
    assert result == Decimal("3520.5")


@patch("revaluation.prices.requests.get")
def test_fetch_yahoo_quote_empty_result_raises(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"quoteResponse": {"result": []}}
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    with pytest.raises(PriceFetchError, match="yahoo_empty_result"):
        fetch_yahoo_quote("GC=F")


@patch("revaluation.prices.requests.get")
def test_fetch_yahoo_quote_no_price_raises(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "quoteResponse": {"result": [{"symbol": "GC=F"}]}
    }
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    with pytest.raises(PriceFetchError, match="yahoo_no_price"):
        fetch_yahoo_quote("GC=F")


# ── get_or_fetch_price (cache behavior) ──────────────────────────


@patch("revaluation.prices.fetch_yahoo_quote")
def test_get_or_fetch_uses_cache_within_ttl(mock_fetch, in_memory_db):
    """Fresh cached price -> no Yahoo call."""
    fresh_time = datetime.now(timezone.utc).isoformat()
    in_memory_db.execute(
        "INSERT INTO commodity_prices (commodity, price_usd, unit, source, fetched_at) VALUES (?, ?, ?, ?, ?)",
        ("Au", 3520.0, "USD/oz", "yahoo:GC=F", fresh_time),
    )
    in_memory_db.commit()

    price, pid = get_or_fetch_price(in_memory_db, "Au")
    assert price == Decimal("3520.0")
    assert pid == 1
    mock_fetch.assert_not_called()


@patch("revaluation.prices.fetch_yahoo_quote")
def test_get_or_fetch_calls_yahoo_when_stale(mock_fetch, in_memory_db):
    """Stale price -> fetches new from Yahoo."""
    stale_time = (datetime.now(timezone.utc) - timedelta(hours=CACHE_TTL_HOURS + 1)).isoformat()
    in_memory_db.execute(
        "INSERT INTO commodity_prices (commodity, price_usd, unit, source, fetched_at) VALUES (?, ?, ?, ?, ?)",
        ("Au", 3400.0, "USD/oz", "yahoo:GC=F", stale_time),
    )
    in_memory_db.commit()

    mock_fetch.return_value = Decimal("3550.0")

    price, pid = get_or_fetch_price(in_memory_db, "Au")
    assert price == Decimal("3550.0")
    assert pid == 2  # new row inserted
    mock_fetch.assert_called_once_with("GC=F")


@patch("revaluation.prices.fetch_yahoo_quote")
def test_get_or_fetch_unknown_commodity_raises(mock_fetch, in_memory_db):
    with pytest.raises(PriceFetchError, match="no_yahoo_symbol"):
        get_or_fetch_price(in_memory_db, "Zn")
