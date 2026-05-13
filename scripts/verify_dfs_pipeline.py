"""
End-to-end verification of the DFS parser.

Inserts a synthetic documents row, runs the production codepath
(_extract_dfs_study from pipeline.orchestrator), and prints
the resulting projects / studies rows.

Usage:
    python -m scripts.verify_dfs_pipeline DEG tests/fixtures/dfs/DEG_dfs_2024-06-15.pdf
"""

import hashlib
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from db import get_connection, init_db


def main():
    if len(sys.argv) < 3:
        print("Usage: python -m scripts.verify_dfs_pipeline <TICKER> <PDF_PATH>")
        sys.exit(1)

    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_API_KEY or GEMINI_API_KEY must be set")
        sys.exit(1)

    ticker = sys.argv[1].upper()
    pdf_path = Path(sys.argv[2])

    if not pdf_path.exists():
        print(f"ERROR: PDF not found: {pdf_path}")
        sys.exit(1)

    init_db()
    pdf_bytes = pdf_path.read_bytes()

    # Parse announcement date from filename (TICKER_dfs_YYYY-MM-DD.pdf)
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", pdf_path.stem)
    announcement_date = date_match.group(1) if date_match else datetime.now().strftime("%Y-%m-%d")

    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()

    # Ensure company exists
    existing = conn.execute("SELECT company_id FROM companies WHERE ticker = ?", (ticker,)).fetchone()
    if not existing:
        conn.execute(
            "INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES (?, ?, ?, ?)",
            (ticker, ticker, now, now),
        )
        conn.commit()
        print(f"Created companies row for {ticker}")

    # Upsert document
    verify_url = f"verify://dfs/{ticker}/{pdf_path.name}"
    doc_sha = hashlib.sha256(f"{ticker}:{verify_url}".encode()).hexdigest()

    existing_doc = conn.execute("SELECT document_id FROM documents WHERE sha256 = ?", (doc_sha,)).fetchone()
    if existing_doc:
        doc_id = existing_doc["document_id"]
        conn.execute(
            "UPDATE documents SET parse_status = 'classified', parse_error = NULL WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()
        print(f"Reset existing document {doc_id}")
    else:
        cursor = conn.execute(
            """INSERT INTO documents (ticker, url, sha256, source, announcement_date, ingested_at,
                                      doc_type, header, parse_status)
               VALUES (?, ?, ?, 'verify', ?, ?, 'study_dfs', ?, 'classified')""",
            (ticker, verify_url, doc_sha, announcement_date, now,
             f"DFS verification: {pdf_path.name}"),
        )
        doc_id = cursor.lastrowid
        conn.commit()
        print(f"Inserted document {doc_id}")

    conn.close()

    # Run the extraction
    from pipeline.orchestrator import _extract_dfs_study
    stats = {"extracted": 0, "skipped": 0, "failed": 0}
    _extract_dfs_study(doc_id, pdf_bytes, ticker, announcement_date, stats)
    print(f"\nExtraction stats: {stats}")

    # Query results
    conn = get_connection()

    doc = conn.execute(
        "SELECT parse_status, parse_error FROM documents WHERE document_id = ?",
        (doc_id,),
    ).fetchone()
    print(f"\nDocument status: {doc['parse_status']}")
    if doc["parse_error"]:
        print(f"Parse error: {doc['parse_error']}")

    company_id = conn.execute(
        "SELECT company_id FROM companies WHERE ticker = ?", (ticker,)
    ).fetchone()["company_id"]

    print(f"\n=== Projects for {ticker} ===")
    projects = conn.execute(
        "SELECT * FROM projects WHERE company_id = ?", (company_id,)
    ).fetchall()
    for p in projects:
        source = p['source'] if 'source' in p.keys() else 'N/A'
        print(f"  [{p['project_id']}] {p['project_name']} | stage={p['stage']} | source={source}")

    print(f"\n=== Studies ===")
    study_count = 0
    for p in projects:
        studies = conn.execute(
            "SELECT * FROM studies WHERE project_id = ?",
            (p["project_id"],),
        ).fetchall()
        for s in studies:
            study_count += 1
            print(f"  [{s['study_id']}] {s['study_stage']} | date={s['study_date']}")
            print(f"    NPV(post-tax)={s['post_tax_npv']}M | NPV(pre-tax)={s['pre_tax_npv']}M")
            print(f"    IRR={s['irr_pct']}% | Capex={s['initial_capex']}M")
            print(f"    Mine life={s['mine_life_years']}y | Recovery={s['recovery_pct']}%")
            print(f"    AISC={s['aisc_per_unit']} {s['aisc_unit']}")
            print(f"    Currency={s['reporting_currency']} | Discount={s['discount_rate_pct']}%")
            print(f"    Model={s['extraction_model']}")

    conn.close()

    if doc["parse_status"] == "parsed" and study_count > 0:
        print(f"\nSUCCESS: {study_count} study rows extracted")
        sys.exit(0)
    else:
        print(f"\nFAILED: parse_status={doc['parse_status']}, study_count={study_count}")
        sys.exit(1)


if __name__ == "__main__":
    main()
