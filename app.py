"""
ASX Junior Miner API

REST API for the valuation pipeline. Serves data to the Next.js frontend.

Usage:
    python app.py                        # dev server on port 8000
    gunicorn app:app -b 0.0.0.0:$PORT    # production (Railway)
"""

import hashlib
import logging
import os
import threading
import traceback
from pathlib import Path

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.utils import secure_filename

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from db import get_connection, init_db
from pipeline.classifier import classify_title
from valuation.engine import valuate_ticker, valuate_all

RAW_DIR = Path(__file__).resolve().parent / "data" / "raw"

app = Flask(__name__)
CORS(app)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-key-change-in-prod")

init_db()


def query_db(sql, params=(), one=False):
    conn = get_connection()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    result = [dict(r) for r in rows]
    return result[0] if one and result else (None if one else result)


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.route("/api/stats")
def api_stats():
    return jsonify({
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
    })


@app.route("/api/companies")
def api_companies():
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
    return jsonify(rows)


@app.route("/api/company/<ticker>")
def api_company(ticker):
    ticker = ticker.upper()
    company = query_db("SELECT * FROM companies WHERE ticker = ?", (ticker,), one=True)
    if not company:
        return jsonify({"error": "Not found"}), 404

    financials = query_db(
        "SELECT * FROM company_financials WHERE ticker = ? ORDER BY effective_date DESC",
        (ticker,),
    )
    projects = query_db("SELECT * FROM projects WHERE ticker = ?", (ticker,))
    resources = query_db(
        """SELECT r.*, p.project_name
           FROM resources r JOIN projects p ON r.project_id = p.id
           WHERE p.ticker = ? ORDER BY r.category""",
        (ticker,),
    )
    studies = query_db(
        """SELECT s.*, p.project_name
           FROM studies s JOIN projects p ON s.project_id = p.id
           WHERE p.ticker = ? ORDER BY s.study_date DESC""",
        (ticker,),
    )
    documents = query_db(
        "SELECT * FROM documents WHERE company_ticker = ? ORDER BY announcement_date DESC",
        (ticker,),
    )
    drill_results = query_db(
        """SELECT dr.hole_id, dr.from_m, dr.to_m, dr.interval_m,
                  dr.au_gt, dr.au_eq_gt, dr.sb_pct, dr.is_including
           FROM drill_results dr JOIN projects p ON dr.project_id = p.id
           WHERE p.ticker = ? ORDER BY dr.hole_id, dr.from_m LIMIT 200""",
        (ticker,),
    )

    valuation = None
    try:
        v = valuate_ticker(ticker)
        valuation = {
            "ticker": v.ticker,
            "stage": v.stage,
            "method": v.method,
            "ev_aud": v.ev_aud,
            "nav_aud": v.nav_aud,
            "nav_per_share": v.nav_per_share,
            "ev_per_resource_unit": v.ev_per_resource_unit,
            "resource_unit": v.resource_unit,
            "total_attributable_resource": v.total_attributable_resource,
            "shares_fd": v.shares_fd,
            "cash_aud": v.cash_aud,
            "debt_aud": v.debt_aud,
            "red_flags": v.red_flags,
        }
    except Exception:
        pass

    return jsonify({
        "company": company,
        "financials": financials,
        "projects": projects,
        "resources": resources,
        "studies": studies,
        "documents": documents,
        "drill_results": drill_results,
        "valuation": valuation,
    })


@app.route("/api/documents")
def api_documents():
    status = request.args.get("status")
    doc_type = request.args.get("type")

    query = "SELECT * FROM documents WHERE 1=1"
    params = []
    if status:
        query += " AND parse_status = ?"
        params.append(status)
    if doc_type:
        query += " AND doc_type = ?"
        params.append(doc_type)
    query += " ORDER BY company_ticker, created_at DESC"

    return jsonify(query_db(query, params))


@app.route("/api/valuations")
def api_valuations():
    results = valuate_all()
    return jsonify([{
        "ticker": v.ticker,
        "stage": v.stage,
        "method": v.method,
        "ev_aud": v.ev_aud,
        "nav_aud": v.nav_aud,
        "nav_per_share": v.nav_per_share,
        "ev_per_resource_unit": v.ev_per_resource_unit,
        "resource_unit": v.resource_unit,
        "total_attributable_resource": v.total_attributable_resource,
        "shares_fd": v.shares_fd,
        "cash_aud": v.cash_aud,
        "debt_aud": v.debt_aud,
        "red_flags": v.red_flags,
    } for v in results])


@app.route("/api/review")
def api_review():
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
    return jsonify(data)


@app.route("/api/resources/<ticker>")
def api_resources(ticker):
    rows = query_db(
        """SELECT r.category, r.contained_metal, r.contained_unit, r.commodity
           FROM resources r JOIN projects p ON r.project_id = p.id
           WHERE p.ticker = ? AND r.category != 'Total' AND r.contained_metal IS NOT NULL""",
        (ticker.upper(),),
    )
    return jsonify(rows)


@app.route("/api/drill/<ticker>")
def api_drill(ticker):
    rows = query_db(
        """SELECT dr.hole_id, dr.from_m, dr.to_m, dr.interval_m,
                  dr.au_gt, dr.au_eq_gt, dr.sb_pct, dr.is_including
           FROM drill_results dr JOIN projects p ON dr.project_id = p.id
           WHERE p.ticker = ? AND dr.is_including = 0
           ORDER BY COALESCE(dr.au_eq_gt, dr.au_gt, 0) * COALESCE(dr.interval_m, 0) DESC
           LIMIT 30""",
        (ticker.upper(),),
    )
    return jsonify(rows)


@app.route("/api/upload", methods=["POST"])
def api_upload():
    """
    Upload PDFs for a ticker. Accepts multipart form data with:
    - ticker: company ticker (required)
    - doc_type: override document type (optional, e.g. appendix_5b, resource_update, study)
    - files: one or more PDF files
    Auto-registers, classifies, parses, normalizes, and loads each file.
    """
    ticker = request.form.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker is required"}), 400

    override_doc_type = request.form.get("doc_type", "").strip()

    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files provided"}), 400

    ticker_dir = RAW_DIR / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)

    uploaded = []
    for f in files:
        if not f.filename or not f.filename.lower().endswith(".pdf"):
            continue
        filename = secure_filename(f.filename)
        save_path = ticker_dir / filename
        f.save(str(save_path))

        # Register in DB
        rel_path = str(save_path.relative_to(Path(__file__).resolve().parent))
        doc_id = hashlib.sha256(rel_path.encode()).hexdigest()[:16]
        header = filename.replace(".pdf", "").replace("-", " ").replace("_", " ")
        doc_type = override_doc_type if override_doc_type else classify_title(header)

        conn = get_connection()
        conn.execute(
            "INSERT OR IGNORE INTO companies (ticker, updated_at) VALUES (?, CURRENT_TIMESTAMP)",
            (ticker,),
        )
        conn.execute(
            """INSERT OR IGNORE INTO documents
               (id, company_ticker, doc_type, header, announcement_date, url, local_path, parse_status)
               VALUES (?, ?, ?, ?, NULL, '', ?, 'pending')""",
            (doc_id, ticker, doc_type, header, rel_path),
        )
        conn.commit()
        conn.close()

        uploaded.append({"filename": filename, "doc_id": doc_id, "doc_type": doc_type})

    if not uploaded:
        return jsonify({"error": "No valid PDF files"}), 400

    # Run pipeline in background for this ticker
    def run_parse():
        try:
            logger.info(f"Starting pipeline for {ticker}")
            from parse import run_pipeline
            run_pipeline(ticker)
            logger.info(f"Pipeline completed for {ticker}")
        except Exception:
            logger.error(f"Pipeline failed for {ticker}:\n{traceback.format_exc()}")

    thread = threading.Thread(target=run_parse, daemon=True)
    thread.start()

    return jsonify({
        "status": "processing",
        "ticker": ticker,
        "files": uploaded,
    })


@app.route("/api/parse", methods=["POST"])
def api_parse():
    ticker = request.json.get("ticker") if request.is_json else None

    def run_parse():
        try:
            logger.info(f"Starting pipeline for {ticker or 'all'}")
            from parse import run_pipeline
            run_pipeline(ticker)
            logger.info(f"Pipeline completed for {ticker or 'all'}")
        except Exception:
            logger.error(f"Pipeline failed for {ticker or 'all'}:\n{traceback.format_exc()}")

    thread = threading.Thread(target=run_parse, daemon=True)
    thread.start()
    return jsonify({"status": "started", "ticker": ticker or "all"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=True)
