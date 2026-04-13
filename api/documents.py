"""
Documents endpoints — GET /api/documents, GET /api/stats
"""

from flask import Blueprint, jsonify, request

from db import get_connection

bp = Blueprint("documents", __name__)


def query_db(sql, params=(), one=False):
    conn = get_connection()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    result = [dict(r) for r in rows]
    return result[0] if one and result else (None if one else result)


@bp.route("/api/stats")
def api_stats():
    return jsonify({
        "companies": query_db("SELECT COUNT(*) as n FROM companies", one=True)["n"],
        "documents": query_db("SELECT COUNT(*) as n FROM documents", one=True)["n"],
        "docs_parsed": query_db("SELECT COUNT(*) as n FROM documents WHERE parse_status='parsed'", one=True)["n"],
        "docs_pending": query_db("SELECT COUNT(*) as n FROM documents WHERE parse_status='pending'", one=True)["n"],
        "docs_classified": query_db("SELECT COUNT(*) as n FROM documents WHERE parse_status='classified'", one=True)["n"],
        "docs_failed": query_db("SELECT COUNT(*) as n FROM documents WHERE parse_status='failed'", one=True)["n"],
        "financials": query_db("SELECT COUNT(*) as n FROM company_financials", one=True)["n"],
        "needs_review": query_db("SELECT COUNT(*) as n FROM company_financials WHERE needs_review=1", one=True)["n"],
    })


@bp.route("/api/companies")
def api_companies():
    rows = query_db("""
        SELECT c.ticker, c.name, c.reporting_currency,
               (SELECT COUNT(*) FROM documents WHERE ticker = c.ticker) as doc_count,
               (SELECT COUNT(*) FROM documents WHERE ticker = c.ticker AND parse_status = 'parsed') as parsed_count
        FROM companies c
        ORDER BY c.ticker
    """)
    return jsonify(rows)


@bp.route("/api/documents")
def api_documents():
    status = request.args.get("status")
    doc_type = request.args.get("type")
    ticker = request.args.get("ticker")

    query = "SELECT * FROM documents WHERE 1=1"
    params = []
    if status:
        query += " AND parse_status = ?"
        params.append(status)
    if doc_type:
        query += " AND doc_type = ?"
        params.append(doc_type)
    if ticker:
        query += " AND ticker = ?"
        params.append(ticker.upper())
    query += " ORDER BY ticker, ingested_at DESC"

    return jsonify(query_db(query, params))
