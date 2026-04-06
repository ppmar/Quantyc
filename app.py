"""
ASX Junior Miner Dashboard

Web dashboard for the valuation pipeline. Shows pipeline status,
company data, resources, financials, valuations, and red flags.

Usage:
    python app.py                        # dev server on port 5000
    gunicorn app:app -b 0.0.0.0:$PORT    # production (Railway)
"""

import os
import json
import subprocess
import threading
from pathlib import Path

from flask import Flask, render_template, jsonify, request, redirect, url_for

from db import get_connection, init_db
from valuation.engine import valuate_ticker, valuate_all, ValuationResult

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-key-change-in-prod")

# Ensure DB is ready
init_db()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def query_db(sql, params=(), one=False):
    conn = get_connection()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    result = [dict(r) for r in rows]
    return result[0] if one and result else (None if one else result)


def fmt_number(val, decimals=0, prefix="", suffix=""):
    if val is None:
        return "-"
    try:
        val = float(val)
    except (ValueError, TypeError):
        return str(val)
    if abs(val) >= 1_000_000_000:
        return f"{prefix}{val/1e9:,.{decimals}f}B{suffix}"
    if abs(val) >= 1_000_000:
        return f"{prefix}{val/1e6:,.{decimals}f}M{suffix}"
    if abs(val) >= 1_000:
        return f"{prefix}{val/1e3:,.{decimals}f}K{suffix}"
    return f"{prefix}{val:,.{decimals}f}{suffix}"


app.jinja_env.globals.update(fmt_number=fmt_number)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def dashboard():
    stats = {
        "companies": query_db("SELECT COUNT(*) as n FROM companies", one=True)["n"],
        "documents": query_db("SELECT COUNT(*) as n FROM documents", one=True)["n"],
        "docs_done": query_db("SELECT COUNT(*) as n FROM documents WHERE parse_status='done'", one=True)["n"],
        "docs_pending": query_db("SELECT COUNT(*) as n FROM documents WHERE parse_status='pending'", one=True)["n"],
        "docs_failed": query_db("SELECT COUNT(*) as n FROM documents WHERE parse_status='failed'", one=True)["n"],
        "staging_rows": query_db("SELECT COUNT(*) as n FROM staging_extractions", one=True)["n"],
        "needs_review": query_db("SELECT COUNT(*) as n FROM staging_extractions WHERE needs_review=1", one=True)["n"],
        "resources": query_db("SELECT COUNT(*) as n FROM resources WHERE category != 'Total'", one=True)["n"],
        "financials": query_db("SELECT COUNT(*) as n FROM company_financials", one=True)["n"],
        "studies": query_db("SELECT COUNT(*) as n FROM studies", one=True)["n"],
        "drill_intercepts": query_db("SELECT COUNT(*) as n FROM drill_results", one=True)["n"],
        "drill_holes": query_db("SELECT COUNT(DISTINCT hole_id) as n FROM drill_results", one=True)["n"],
    }

    docs_by_type = query_db(
        "SELECT doc_type, COUNT(*) as n FROM documents GROUP BY doc_type ORDER BY n DESC"
    )

    docs_by_status = query_db(
        "SELECT parse_status, COUNT(*) as n FROM documents GROUP BY parse_status"
    )

    recent_docs = query_db(
        """SELECT id, company_ticker, doc_type, header, parse_status, announcement_date
           FROM documents ORDER BY created_at DESC LIMIT 10"""
    )

    # Red flags
    from review.exceptions import check_red_flags
    conn = get_connection()
    red_flags = check_red_flags(conn)
    conn.close()

    return render_template("dashboard.html",
                           stats=stats,
                           docs_by_type=docs_by_type,
                           docs_by_status=docs_by_status,
                           recent_docs=recent_docs,
                           red_flags=red_flags)


@app.route("/companies")
def companies():
    rows = query_db("""
        SELECT c.ticker, c.name, c.primary_commodity,
               (SELECT COUNT(*) FROM documents WHERE company_ticker = c.ticker) as doc_count,
               (SELECT COUNT(*) FROM documents WHERE company_ticker = c.ticker AND parse_status = 'done') as parsed_count,
               (SELECT stage FROM projects WHERE ticker = c.ticker AND is_primary = 1 LIMIT 1) as stage,
               (SELECT cash_aud FROM company_financials WHERE ticker = c.ticker ORDER BY effective_date DESC LIMIT 1) as cash,
               (SELECT cash_runway_months FROM company_financials WHERE ticker = c.ticker ORDER BY effective_date DESC LIMIT 1) as runway
        FROM companies c
        ORDER BY c.ticker
    """)
    return render_template("companies.html", companies=rows)


@app.route("/company/<ticker>")
def company_detail(ticker):
    ticker = ticker.upper()

    company = query_db("SELECT * FROM companies WHERE ticker = ?", (ticker,), one=True)
    if not company:
        return render_template("404.html", message=f"Company {ticker} not found"), 404

    financials = query_db(
        """SELECT * FROM company_financials WHERE ticker = ?
           ORDER BY effective_date DESC""",
        (ticker,),
    )

    projects = query_db("SELECT * FROM projects WHERE ticker = ?", (ticker,))

    resources = query_db(
        """SELECT r.*, p.project_name
           FROM resources r
           JOIN projects p ON r.project_id = p.id
           WHERE p.ticker = ?
           ORDER BY r.category""",
        (ticker,),
    )

    studies = query_db(
        """SELECT s.*, p.project_name
           FROM studies s
           JOIN projects p ON s.project_id = p.id
           WHERE p.ticker = ?
           ORDER BY s.study_date DESC""",
        (ticker,),
    )

    documents = query_db(
        """SELECT * FROM documents WHERE company_ticker = ?
           ORDER BY announcement_date DESC""",
        (ticker,),
    )

    drill_results = query_db(
        """SELECT dr.hole_id, dr.from_m, dr.to_m, dr.interval_m,
                  dr.au_gt, dr.au_eq_gt, dr.sb_pct, dr.is_including
           FROM drill_results dr
           JOIN projects p ON dr.project_id = p.id
           WHERE p.ticker = ?
           ORDER BY dr.hole_id, dr.from_m
           LIMIT 200""",
        (ticker,),
    )

    # Run valuation
    valuation = None
    try:
        valuation = valuate_ticker(ticker)
    except Exception:
        pass

    return render_template("company_detail.html",
                           company=company,
                           ticker=ticker,
                           financials=financials,
                           projects=projects,
                           resources=resources,
                           studies=studies,
                           documents=documents,
                           drill_results=drill_results,
                           valuation=valuation)


@app.route("/documents")
def documents():
    status_filter = request.args.get("status")
    type_filter = request.args.get("type")

    query = "SELECT * FROM documents WHERE 1=1"
    params = []
    if status_filter:
        query += " AND parse_status = ?"
        params.append(status_filter)
    if type_filter:
        query += " AND doc_type = ?"
        params.append(type_filter)
    query += " ORDER BY company_ticker, created_at DESC"

    docs = query_db(query, params)

    statuses = query_db("SELECT DISTINCT parse_status FROM documents ORDER BY parse_status")
    types = query_db("SELECT DISTINCT doc_type FROM documents ORDER BY doc_type")

    return render_template("documents.html",
                           documents=docs,
                           statuses=[s["parse_status"] for s in statuses],
                           types=[t["doc_type"] for t in types],
                           current_status=status_filter,
                           current_type=type_filter)


@app.route("/valuations")
def valuations():
    results = valuate_all()
    return render_template("valuations.html", valuations=results)


@app.route("/review")
def review():
    from review.exceptions import (
        get_flagged_staging, get_flagged_financials, get_flagged_resources,
        get_flagged_studies, get_failed_documents, check_red_flags
    )
    conn = get_connection()
    data = {
        "staging": get_flagged_staging(conn),
        "financials": get_flagged_financials(conn),
        "resources": get_flagged_resources(conn),
        "studies": get_flagged_studies(conn),
        "failed_docs": get_failed_documents(conn),
        "red_flags": check_red_flags(conn),
    }
    conn.close()
    return render_template("review.html", **data)


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.route("/api/parse", methods=["POST"])
def api_parse():
    """Trigger the parse pipeline. Runs in background."""
    ticker = request.json.get("ticker") if request.is_json else None

    def run_parse():
        from parse import run_pipeline
        run_pipeline(ticker)

    thread = threading.Thread(target=run_parse)
    thread.start()

    return jsonify({"status": "started", "ticker": ticker or "all"})


@app.route("/api/stats")
def api_stats():
    stats = {
        "companies": query_db("SELECT COUNT(*) as n FROM companies", one=True)["n"],
        "documents": query_db("SELECT COUNT(*) as n FROM documents", one=True)["n"],
        "docs_done": query_db("SELECT COUNT(*) as n FROM documents WHERE parse_status='done'", one=True)["n"],
        "docs_pending": query_db("SELECT COUNT(*) as n FROM documents WHERE parse_status='pending'", one=True)["n"],
        "docs_failed": query_db("SELECT COUNT(*) as n FROM documents WHERE parse_status='failed'", one=True)["n"],
    }
    return jsonify(stats)


@app.route("/api/resources/<ticker>")
def api_resources(ticker):
    """Chart data for resource breakdown."""
    rows = query_db(
        """SELECT r.category, r.contained_metal, r.contained_unit, r.commodity
           FROM resources r
           JOIN projects p ON r.project_id = p.id
           WHERE p.ticker = ? AND r.category != 'Total' AND r.contained_metal IS NOT NULL""",
        (ticker.upper(),),
    )
    return jsonify(rows)


@app.route("/api/drill/<ticker>")
def api_drill(ticker):
    """Chart data for drill results."""
    rows = query_db(
        """SELECT dr.hole_id, dr.from_m, dr.to_m, dr.interval_m,
                  dr.au_gt, dr.au_eq_gt, dr.sb_pct, dr.is_including
           FROM drill_results dr
           JOIN projects p ON dr.project_id = p.id
           WHERE p.ticker = ? AND dr.is_including = 0
           ORDER BY COALESCE(dr.au_eq_gt, dr.au_gt, 0) * COALESCE(dr.interval_m, 0) DESC
           LIMIT 30""",
        (ticker.upper(),),
    )
    return jsonify(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
