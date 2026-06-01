"""
Revaluation backfill endpoint.

POST /api/revalue/backfill — compute revaluations for existing studies that
have none yet. The orchestrator only auto-triggers revaluation when a DFS is
freshly parsed, so studies imported via sync (or carried over a volume reset)
never get revalued. This endpoint closes that gap and is idempotent.
"""

import logging

from flask import Blueprint, jsonify, request

from db import get_connection
from revaluation.pipeline import revalue_study

logger = logging.getLogger(__name__)

bp = Blueprint("revalue", __name__)

# Only definitive/indicative studies are revalued (matches orchestrator policy).
_BACKFILL_TIERS = ("definitive", "indicative")


@bp.route("/api/revalue/backfill", methods=["POST"])
def revalue_backfill():
    """
    Revalue every definitive/indicative study lacking a revaluation row.

    Optional JSON body:
        { "ticker": "WAF" }   — restrict to one ticker
    """
    data = request.get_json(silent=True) or {}
    ticker = (data.get("ticker") or "").upper().strip()

    conn = get_connection()

    sql = """
        SELECT s.study_id, c.ticker
        FROM studies s
        JOIN projects p  ON p.project_id = s.project_id
        JOIN companies c ON c.company_id = p.company_id
        WHERE s.study_confidence_tier IN (?, ?)
          AND s.study_id NOT IN (SELECT study_id FROM revaluations WHERE study_id IS NOT NULL)
    """
    params = list(_BACKFILL_TIERS)
    if ticker:
        sql += " AND c.ticker = ?"
        params.append(ticker)
    sql += " ORDER BY c.ticker, s.study_id"

    candidates = conn.execute(sql, params).fetchall()

    revalued, skipped, errors = [], [], []
    for row in candidates:
        sid = row["study_id"]
        try:
            reval_id = revalue_study(conn, sid)
            if reval_id:
                revalued.append({"study_id": sid, "ticker": row["ticker"], "revaluation_id": reval_id})
            else:
                skipped.append({"study_id": sid, "ticker": row["ticker"], "reason": "unsupported_commodity"})
        except Exception as e:
            errors.append({"study_id": sid, "ticker": row["ticker"], "error": str(e)})
            logger.warning("Backfill revaluation failed for study %d (%s): %s", sid, row["ticker"], e)

    conn.close()

    return jsonify({
        "candidates": len(candidates),
        "revalued_count": len(revalued),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "revalued": revalued,
        "skipped": skipped,
        "errors": errors,
    })
