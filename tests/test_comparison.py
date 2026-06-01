"""Tests for the price-comparison endpoint and Yahoo history parsing."""
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest


def _epoch(iso: str) -> int:
    return int(datetime.strptime(iso, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


# ── fetch_yahoo_history parsing ───────────────────────────────────

def test_fetch_yahoo_history_skips_null_closes():
    from revaluation import prices
    payload = {
        "chart": {"result": [{
            "timestamp": [_epoch("2025-01-01"), _epoch("2025-01-02"), _epoch("2025-01-03")],
            "indicators": {"quote": [{"close": [10.0, None, 12.0]}]},
        }]}
    }
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    with patch.object(prices.requests, "get", return_value=resp):
        out = prices.fetch_yahoo_history("X.AX", 0, 1, "1d")
    assert out == [("2025-01-01", 10.0), ("2025-01-03", 12.0)]


def test_fetch_yahoo_history_empty_raises():
    from revaluation import prices
    resp = MagicMock()
    resp.json.return_value = {"chart": {"result": []}}
    resp.raise_for_status.return_value = None
    with patch.object(prices.requests, "get", return_value=resp):
        with pytest.raises(prices.PriceFetchError):
            prices.fetch_yahoo_history("BAD", 0, 1, "1d")


# ── endpoint alignment / forward-fill ─────────────────────────────

@pytest.fixture
def client():
    import app as app_module
    app_module.app.config["TESTING"] = True
    from api import comparison
    comparison._CACHE.clear()
    with app_module.app.test_client() as c:
        yield c


def test_price_comparison_aligns_and_forward_fills(client):
    # Stock trades 4 days; commodity only 2 (gaps) -> forward-fill, trim leading.
    stock = [("2025-01-01", 1.0), ("2025-01-02", 1.1), ("2025-01-03", 1.2), ("2025-01-04", 1.3)]
    comm = [("2025-01-02", 100.0), ("2025-01-03", 110.0)]

    def fake_hist(symbol, p1, p2, interval):
        return stock if symbol.endswith(".AX") else comm

    with patch("api.comparison.fetch_yahoo_history", side_effect=fake_hist):
        resp = client.get("/api/company/WAF/price-comparison?commodity=Au&range=1y")
    data = resp.get_json()
    assert resp.status_code == 200
    assert data["stock_symbol"] == "WAF.AX"
    assert data["commodity_symbol"] == "GC=F"
    s = data["series"]
    # 2025-01-01 dropped (no commodity baseline yet)
    assert [pt["date"] for pt in s] == ["2025-01-02", "2025-01-03", "2025-01-04"]
    # 2025-01-04 forward-fills commodity from 01-03
    assert s[-1]["commodity"] == 110.0
    assert s[-1]["stock"] == 1.3


def test_price_comparison_rejects_unsupported_commodity(client):
    resp = client.get("/api/company/WAF/price-comparison?commodity=U3O8&range=1y")
    assert resp.status_code == 400
    assert "unsupported_commodity" in resp.get_json()["error"]
