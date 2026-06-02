"""Ingest-health dashboard — GET /api/health/ingest."""
import re
from datetime import datetime, timezone

from flask import Blueprint, jsonify

from db import get_connection

bp = Blueprint("health", __name__)

_STUDY_TYPES = ("study_dfs", "study_pfs", "study_scoping")


def _bucket(error: str) -> str:
    """Collapse a parse_error to a short stable prefix for grouping."""
    return re.split(r"[:(]", error or "none")[0][:60]


@bp.route("/api/health/ingest")
def ingest_health():
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()

    def scalar(sql, params=()):
        return conn.execute(sql, params).fetchone()[0]

    totals = {
        "documents": scalar("SELECT COUNT(*) FROM documents"),
        "parsed": scalar("SELECT COUNT(*) FROM documents WHERE parse_status='parsed'"),
        "failed": scalar("SELECT COUNT(*) FROM documents WHERE parse_status='failed'"),
        "retry_scheduled": scalar("SELECT COUNT(*) FROM documents WHERE parse_status='retry_scheduled'"),
        "classified": scalar("SELECT COUNT(*) FROM documents WHERE parse_status='classified'"),
        "pending": scalar("SELECT COUNT(*) FROM documents WHERE parse_status='pending'"),
        "skipped": scalar("SELECT COUNT(*) FROM documents WHERE parse_status='skipped'"),
    }

    failures_by_class = {
        "transient": scalar("SELECT COUNT(*) FROM documents WHERE parse_status='failed' AND failure_class='transient'"),
        "permanent": scalar("SELECT COUNT(*) FROM documents WHERE parse_status='failed' AND failure_class='permanent'"),
        "unclassified": scalar("SELECT COUNT(*) FROM documents WHERE parse_status='failed' AND failure_class IS NULL"),
    }

    retry_queue = {
        "scheduled": totals["retry_scheduled"],
        "due_now": scalar("SELECT COUNT(*) FROM documents WHERE parse_status='retry_scheduled' AND next_retry_at <= ?", (now,)),
        "next_at": scalar("SELECT MIN(next_retry_at) FROM documents WHERE parse_status='retry_scheduled'"),
    }

    ph = ",".join("?" * len(_STUDY_TYPES))
    with_study_doc = scalar(
        f"SELECT COUNT(DISTINCT ticker) FROM documents WHERE doc_type IN ({ph})",
        _STUDY_TYPES,
    )
    with_parsed_study = scalar(
        f"SELECT COUNT(DISTINCT ticker) FROM documents WHERE doc_type IN ({ph}) AND parse_status='parsed'",
        _STUDY_TYPES,
    )
    study_coverage = {
        "companies_with_study_doc": with_study_doc,
        "companies_with_parsed_study": with_parsed_study,
        "recoverable_gap": with_study_doc - with_parsed_study,
    }

    rows = conn.execute(
        "SELECT doc_type, parse_error FROM documents WHERE parse_status IN ('failed','retry_scheduled')"
    ).fetchall()
    counts: dict[tuple[str, str], int] = {}
    for r in rows:
        key = (_bucket(r["parse_error"]), r["doc_type"] or "?")
        counts[key] = counts.get(key, 0) + 1
    error_buckets = sorted(
        ({"reason": k[0], "doc_type": k[1], "count": v} for k, v in counts.items()),
        key=lambda x: x["count"], reverse=True,
    )[:25]

    conn.close()
    return jsonify({
        "totals": totals,
        "failures_by_class": failures_by_class,
        "retry_queue": retry_queue,
        "study_coverage": study_coverage,
        "error_buckets": error_buckets,
        "as_of": now,
    })
