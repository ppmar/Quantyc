"""
Financials endpoints — GET /api/companies/<ticker>/financials
"""

from flask import Blueprint, jsonify

from db import get_connection

bp = Blueprint("financials", __name__)


@bp.route("/api/companies/<ticker>/financials")
def api_company_financials(ticker):
    ticker = ticker.upper()
    conn = get_connection()

    # Latest snapshot
    latest = conn.execute(
        """SELECT cf.*, c.ticker, c.name, c.reporting_currency
           FROM company_financials cf
           JOIN companies c ON cf.company_id = c.company_id
           WHERE c.ticker = ?
           ORDER BY cf.effective_date DESC
           LIMIT 1""",
        (ticker,),
    ).fetchone()

    if not latest:
        conn.close()
        return jsonify({"error": "No financials found for ticker"}), 404

    # History
    history = conn.execute(
        """SELECT cf.*
           FROM company_financials cf
           JOIN companies c ON cf.company_id = c.company_id
           WHERE c.ticker = ?
           ORDER BY cf.effective_date DESC""",
        (ticker,),
    ).fetchall()

    conn.close()

    return jsonify({
        "ticker": ticker,
        "latest": dict(latest),
        "history": [dict(r) for r in history],
    })
