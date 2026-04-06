"""
Exception Review

Surfaces all records flagged with needs_review=True across the database.
Provides a consolidated view for manual review and correction.

Usage:
    python -m review.exceptions
    python -m review.exceptions --ticker DEG
    python -m review.exceptions --format csv
"""

import argparse
import logging
import sys

from db import get_connection, init_db

logger = logging.getLogger(__name__)


def get_flagged_staging(conn, ticker: str | None = None) -> list[dict]:
    """Get all staging extractions flagged for review."""
    query = """
        SELECT s.id, s.document_id, d.company_ticker, d.doc_type, d.header,
               s.field_name, s.raw_value, s.normalized_value, s.unit,
               s.extraction_method, s.confidence
        FROM staging_extractions s
        JOIN documents d ON s.document_id = d.id
        WHERE s.needs_review = 1 AND s.reviewed = 0
    """
    params = []
    if ticker:
        query += " AND d.company_ticker = ?"
        params.append(ticker.upper())
    query += " ORDER BY d.company_ticker, d.announcement_date DESC"

    return [dict(r) for r in conn.execute(query, params).fetchall()]


def get_flagged_financials(conn, ticker: str | None = None) -> list[dict]:
    """Get company financials flagged for review."""
    query = """
        SELECT cf.id, cf.ticker, cf.effective_date, cf.cash_aud,
               cf.quarterly_burn, cf.cash_runway_months,
               cf.shares_basic, cf.shares_fd,
               cf.extraction_method, cf.confidence, cf.source_doc_id
        FROM company_financials cf
        WHERE cf.needs_review = 1
    """
    params = []
    if ticker:
        query += " AND cf.ticker = ?"
        params.append(ticker.upper())
    query += " ORDER BY cf.ticker, cf.effective_date DESC"

    return [dict(r) for r in conn.execute(query, params).fetchall()]


def get_flagged_resources(conn, ticker: str | None = None) -> list[dict]:
    """Get resource records flagged for review."""
    query = """
        SELECT r.id, p.ticker, r.project_id, r.commodity, r.category,
               r.tonnes_mt, r.grade, r.grade_unit,
               r.contained_metal, r.contained_unit,
               r.extraction_method, r.confidence, r.source_doc_id
        FROM resources r
        JOIN projects p ON r.project_id = p.id
        WHERE r.needs_review = 1
    """
    params = []
    if ticker:
        query += " AND p.ticker = ?"
        params.append(ticker.upper())
    query += " ORDER BY p.ticker, r.effective_date DESC"

    return [dict(r) for r in conn.execute(query, params).fetchall()]


def get_flagged_studies(conn, ticker: str | None = None) -> list[dict]:
    """Get study records flagged for review."""
    query = """
        SELECT s.id, p.ticker, s.project_id, s.study_stage, s.study_date,
               s.post_tax_npv_musd, s.irr_pct,
               s.assumed_commodity_price, s.assumed_price_unit,
               s.extraction_method, s.confidence, s.source_doc_id
        FROM studies s
        JOIN projects p ON s.project_id = p.id
        WHERE s.needs_review = 1
    """
    params = []
    if ticker:
        query += " AND p.ticker = ?"
        params.append(ticker.upper())
    query += " ORDER BY p.ticker, s.study_date DESC"

    return [dict(r) for r in conn.execute(query, params).fetchall()]


def get_failed_documents(conn, ticker: str | None = None) -> list[dict]:
    """Get documents that failed parsing."""
    query = """
        SELECT id, company_ticker, doc_type, header, announcement_date, url
        FROM documents
        WHERE parse_status = 'failed'
    """
    params = []
    if ticker:
        query += " AND company_ticker = ?"
        params.append(ticker.upper())
    query += " ORDER BY company_ticker, announcement_date DESC"

    return [dict(r) for r in conn.execute(query, params).fetchall()]


def check_red_flags(conn, ticker: str | None = None) -> list[dict]:
    """
    Run red flag checks across the database.
    Returns list of {ticker, flag_type, description} dicts.
    """
    flags = []

    # Cash runway < 6 months
    query = "SELECT ticker, cash_runway_months FROM company_financials WHERE cash_runway_months < 6"
    params = []
    if ticker:
        query += " AND ticker = ?"
        params.append(ticker.upper())

    for r in conn.execute(query, params).fetchall():
        flags.append({
            "ticker": r["ticker"],
            "flag_type": "low_cash_runway",
            "description": f"Cash runway is {r['cash_runway_months']:.1f} months (< 6 months)",
        })

    # Resource > 70% Inferred
    ticker_clause = f"AND p.ticker = '{ticker.upper()}'" if ticker else ""
    inferred_query = f"""
        SELECT p.ticker,
               SUM(CASE WHEN r.category = 'Inferred' THEN COALESCE(r.contained_metal, 0) ELSE 0 END) as inferred,
               SUM(CASE WHEN r.category != 'Total' THEN COALESCE(r.contained_metal, 0) ELSE 0 END) as total
        FROM resources r
        JOIN projects p ON r.project_id = p.id
        WHERE r.contained_metal IS NOT NULL {ticker_clause}
        GROUP BY p.ticker
    """
    for r in conn.execute(inferred_query).fetchall():
        total = r["total"]
        inferred = r["inferred"]
        if total > 0 and inferred / total > 0.7:
            flags.append({
                "ticker": r["ticker"],
                "flag_type": "high_inferred",
                "description": f"Resource is {inferred/total*100:.0f}% Inferred category",
            })

    # Study commodity price > 20% below current spot
    macro = conn.execute(
        "SELECT gold_spot_usd, copper_spot_usd, silver_spot_usd, lithium_spot_usd FROM macro_assumptions ORDER BY date DESC LIMIT 1"
    ).fetchone()

    if macro:
        study_clause = f"AND p.ticker = '{ticker.upper()}'" if ticker else ""
        studies = conn.execute(f"""
            SELECT p.ticker, s.assumed_commodity_price, s.assumed_price_unit, s.study_stage
            FROM studies s
            JOIN projects p ON s.project_id = p.id
            WHERE s.assumed_commodity_price IS NOT NULL {study_clause}
        """).fetchall()

        for s in studies:
            assumed = s["assumed_commodity_price"]
            unit = (s["assumed_price_unit"] or "").lower()
            current = None
            if "gold" in unit or "/oz" in unit:
                current = macro["gold_spot_usd"]
            elif "copper" in unit or "/lb" in unit:
                current = macro["copper_spot_usd"]
            elif "silver" in unit:
                current = macro["silver_spot_usd"]
            elif "lithium" in unit:
                current = macro["lithium_spot_usd"]

            if current and assumed and current > assumed * 1.2:
                flags.append({
                    "ticker": s["ticker"],
                    "flag_type": "stale_study_price",
                    "description": (
                        f"Study assumed {assumed} {s['assumed_price_unit']} "
                        f"but current spot is {current} (>20% higher)"
                    ),
                })

    # Study older than 3 years
    study_age_clause = f"AND p.ticker = '{ticker.upper()}'" if ticker else ""
    old_studies = conn.execute(f"""
        SELECT p.ticker, s.study_stage, s.study_date
        FROM studies s
        JOIN projects p ON s.project_id = p.id
        WHERE s.study_date IS NOT NULL
          AND julianday('now') - julianday(s.study_date) > 3 * 365
          {study_age_clause}
    """).fetchall()
    for s in old_studies:
        flags.append({
            "ticker": s["ticker"],
            "flag_type": "stale_study",
            "description": f"{s['study_stage']} study dated {s['study_date']} is older than 3 years",
        })

    # Heavy dilution: shares_fd > shares_basic * 1.5
    dilution_clause = f"AND ticker = '{ticker.upper()}'" if ticker else ""
    diluted = conn.execute(f"""
        SELECT ticker, shares_basic, shares_fd
        FROM company_financials
        WHERE shares_basic IS NOT NULL AND shares_fd IS NOT NULL
          AND shares_fd > shares_basic * 1.5
          {dilution_clause}
    """).fetchall()
    for r in diluted:
        ratio = r["shares_fd"] / r["shares_basic"]
        flags.append({
            "ticker": r["ticker"],
            "flag_type": "heavy_dilution",
            "description": f"Fully diluted shares are {ratio:.1f}x basic shares",
        })

    return flags


def print_report(ticker: str | None = None):
    """Print a full exception/review report."""
    conn = get_connection()

    print("\n" + "=" * 70)
    print("  EXCEPTION & REVIEW REPORT")
    if ticker:
        print(f"  Filtered: {ticker.upper()}")
    print("=" * 70)

    # Failed documents
    failed = get_failed_documents(conn, ticker)
    if failed:
        print(f"\n--- FAILED DOCUMENTS ({len(failed)}) ---")
        for f in failed:
            print(f"  [{f['company_ticker']}] {f['doc_type']}: {f['header']}")
            print(f"    ID: {f['id']}  Date: {f['announcement_date']}")

    # Staging extractions needing review
    staging = get_flagged_staging(conn, ticker)
    if staging:
        print(f"\n--- STAGING EXTRACTIONS NEEDING REVIEW ({len(staging)}) ---")
        for s in staging:
            print(f"  [{s['company_ticker']}] {s['field_name']}: {s['raw_value']} ({s['unit']})")
            print(f"    Method: {s['extraction_method']}  Confidence: {s['confidence']}  Doc: {s['document_id'][:12]}...")

    # Flagged financials
    financials = get_flagged_financials(conn, ticker)
    if financials:
        print(f"\n--- FINANCIALS NEEDING REVIEW ({len(financials)}) ---")
        for f in financials:
            print(f"  [{f['ticker']}] Date: {f['effective_date']}  Cash: {f['cash_aud']}  Runway: {f['cash_runway_months']}")

    # Flagged resources
    resources = get_flagged_resources(conn, ticker)
    if resources:
        print(f"\n--- RESOURCES NEEDING REVIEW ({len(resources)}) ---")
        for r in resources:
            print(f"  [{r['ticker']}] {r['commodity']} {r['category']}: {r['contained_metal']} {r['contained_unit']}")
            print(f"    Method: {r['extraction_method']}  Confidence: {r['confidence']}")

    # Flagged studies
    studies = get_flagged_studies(conn, ticker)
    if studies:
        print(f"\n--- STUDIES NEEDING REVIEW ({len(studies)}) ---")
        for s in studies:
            print(f"  [{s['ticker']}] {s['study_stage']} ({s['study_date']}): NPV={s['post_tax_npv_musd']}  IRR={s['irr_pct']}%")

    # Red flags
    flags = check_red_flags(conn, ticker)
    if flags:
        print(f"\n--- RED FLAGS ({len(flags)}) ---")
        for f in flags:
            print(f"  [{f['ticker']}] {f['flag_type']}: {f['description']}")

    # Summary
    total_issues = len(failed) + len(staging) + len(financials) + len(resources) + len(studies) + len(flags)
    print(f"\n{'='*70}")
    print(f"  TOTAL ITEMS FOR REVIEW: {total_issues}")
    print(f"{'='*70}\n")

    conn.close()
    return total_issues


def export_csv(ticker: str | None = None, output: str = "review_report.csv"):
    """Export review items to CSV."""
    import csv

    conn = get_connection()
    rows = []

    for s in get_flagged_staging(conn, ticker):
        rows.append({
            "type": "staging",
            "ticker": s["company_ticker"],
            "field": s["field_name"],
            "value": s["raw_value"],
            "unit": s["unit"],
            "method": s["extraction_method"],
            "confidence": s["confidence"],
            "doc_id": s["document_id"],
        })

    for f in check_red_flags(conn, ticker):
        rows.append({
            "type": "red_flag",
            "ticker": f["ticker"],
            "field": f["flag_type"],
            "value": f["description"],
            "unit": "",
            "method": "",
            "confidence": "",
            "doc_id": "",
        })

    conn.close()

    if not rows:
        print("No review items to export")
        return

    with open(output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Exported {len(rows)} review items to {output}")


def main():
    parser = argparse.ArgumentParser(description="Review flagged exceptions")
    parser.add_argument("--ticker", type=str, help="Filter by ticker")
    parser.add_argument("--format", choices=["text", "csv"], default="text", help="Output format")
    parser.add_argument("--output", type=str, default="review_report.csv", help="CSV output path")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    if args.format == "csv":
        export_csv(args.ticker, args.output)
    else:
        print_report(args.ticker)


if __name__ == "__main__":
    main()
