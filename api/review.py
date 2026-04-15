"""
Review endpoints — GET /api/review, PATCH /api/review/<id>
"""

from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from db import get_connection

bp = Blueprint("review", __name__)


@bp.route("/api/review")
def api_review_list():
    """List flagged company_financials rows with document context."""
    limit = request.args.get("limit", 50, type=int)
    conn = get_connection()
    rows = conn.execute(
        """SELECT cf.financial_id, cf.effective_date, cf.announcement_date,
                  cf.shares_basic, cf.shares_fd, cf.options_outstanding,
                  cf.cash, cf.debt, cf.quarterly_opex_burn, cf.quarterly_invest_burn,
                  cf.review_reason,
                  c.ticker, d.url, d.header
           FROM company_financials cf
           JOIN companies c ON cf.company_id = c.company_id
           JOIN documents d ON cf.document_id = d.document_id
           WHERE cf.needs_review = 1
           ORDER BY cf.created_at DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@bp.route("/api/review/<int:financial_id>", methods=["PATCH"])
def api_review_override(financial_id):
    """Human override — clears review flag."""
    data = request.get_json(silent=True) or {}

    conn = get_connection()
    existing = conn.execute(
        "SELECT financial_id FROM company_financials WHERE financial_id = ?",
        (financial_id,),
    ).fetchone()

    if not existing:
        conn.close()
        return jsonify({"error": "Not found"}), 404

    now = datetime.now(timezone.utc).isoformat()

    # Allow overriding specific fields
    updates = []
    params = []
    for field in ("shares_basic", "shares_fd", "options_outstanding",
                  "perf_rights_outstanding", "cash", "debt",
                  "quarterly_opex_burn", "quarterly_invest_burn"):
        if field in data:
            updates.append(f"{field} = ?")
            params.append(data[field])

    updates.extend([
        "needs_review = 0",
        "reviewed_at = ?",
    ])
    params.append(now)
    params.append(financial_id)

    conn.execute(
        f"UPDATE company_financials SET {', '.join(updates)} WHERE financial_id = ?",
        params,
    )
    conn.commit()
    conn.close()

    return jsonify({"status": "updated", "financial_id": financial_id})
