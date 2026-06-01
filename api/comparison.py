"""
Price comparison endpoint — stock vs commodity history for a company.

GET /api/company/<ticker>/price-comparison?commodity=Au&range=1y

Returns aligned daily closes for the ASX stock (<TICKER>.AX) and the chosen
commodity future, so the frontend can overlay them (rebased to 100). Series are
raw closes; the frontend does the rebasing.
"""

import threading
import time
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from revaluation.prices import fetch_yahoo_history, PriceFetchError

bp = Blueprint("comparison", __name__)

# Commodity -> Yahoo futures symbol. Only those with a feed are chartable.
_COMMODITY_SYMBOL = {"Au": "GC=F", "Ag": "SI=F", "Cu": "HG=F"}

# range -> (seconds back from now, interval). Weekly bars for long windows.
_DAY = 86400
_RANGE = {
    "6m":  (182 * _DAY, "1d"),
    "1y":  (365 * _DAY, "1d"),
    "3y":  (3 * 365 * _DAY, "1wk"),
    "5y":  (5 * 365 * _DAY, "1wk"),
    "max": (30 * 365 * _DAY, "1wk"),
}

# In-memory TTL cache: key -> (fetched_at, series). Per-worker; good enough.
_CACHE: dict[str, tuple[float, list]] = {}
_CACHE_TTL = 6 * 3600
_CACHE_LOCK = threading.Lock()


def _cached_history(symbol: str, range_key: str, period1: int, period2: int, interval: str):
    key = f"{symbol}:{range_key}"
    now = time.time()
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit and now - hit[0] < _CACHE_TTL:
            return hit[1]
    series = fetch_yahoo_history(symbol, period1, period2, interval)
    with _CACHE_LOCK:
        _CACHE[key] = (now, series)
    return series


@bp.route("/api/company/<ticker>/price-comparison")
def price_comparison(ticker: str):
    ticker = ticker.upper().strip()
    commodity = (request.args.get("commodity") or "").strip()
    range_key = (request.args.get("range") or "1y").lower()

    if commodity not in _COMMODITY_SYMBOL:
        return jsonify({"error": f"unsupported_commodity:{commodity}", "series": []}), 400
    if range_key not in _RANGE:
        range_key = "1y"

    span, interval = _RANGE[range_key]
    period2 = int(time.time())
    period1 = period2 - span

    commodity_symbol = _COMMODITY_SYMBOL[commodity]
    stock_symbol = f"{ticker}.AX"

    try:
        stock = _cached_history(stock_symbol, range_key, period1, period2, interval)
        comm = _cached_history(commodity_symbol, range_key, period1, period2, interval)
    except PriceFetchError as e:
        return jsonify({"error": str(e), "series": []}), 502

    # Align on stock trading days; forward-fill commodity from its last close.
    comm_by_date = dict(comm)
    comm_dates = [d for d, _ in comm]
    series = []
    ci = 0
    last_comm = None
    for d, s_close in stock:
        # advance commodity pointer to the latest date <= d
        while ci < len(comm_dates) and comm_dates[ci] <= d:
            last_comm = comm_by_date[comm_dates[ci]]
            ci += 1
        series.append({"date": d, "stock": s_close, "commodity": last_comm})

    # Drop leading points before the commodity series starts (no baseline yet).
    series = [pt for pt in series if pt["commodity"] is not None]

    return jsonify({
        "ticker": ticker,
        "commodity": commodity,
        "range": range_key,
        "stock_symbol": stock_symbol,
        "commodity_symbol": commodity_symbol,
        "as_of": datetime.now(timezone.utc).isoformat(),
        "series": series,
    })
