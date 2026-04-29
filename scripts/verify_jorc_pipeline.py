"""
End-to-end verification of the JORC pipeline on a single fixture.

Inserts a synthetic documents row, runs the production codepath
(_extract_resource_update from pipeline.orchestrator), and prints
the resulting projects / project_commodities / resources rows.

Usage:
    python -m scripts.verify_jorc_pipeline DEG tests/fixtures/jorc_resource_estimate/DEG_mre_2024-12-31.pdf
"""

import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from db import get_connection, init_db


def main():
    if len(sys.argv) < 3:
        print("Usage: python -m scripts.verify_jorc_pipeline <TICKER> <PDF_PATH>")
        sys.exit(1)

    ticker = sys.argv[1].upper()
    pdf_path = Path(sys.argv[2])

    if not pdf_path.exists():
        print(f"ERROR: PDF not found: {pdf_path}")
        sys.exit(1)

    init_db()
    pdf_bytes = pdf_path.read_bytes()
    sha256 = hashlib.sha256(pdf_bytes).hexdigest()

    # Parse announcement date from filename (TICKER_mre_YYYY-MM-DD.pdf)
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
    verify_url = f"verify://jorc/{ticker}/{pdf_path.name}"
    doc_sha = hashlib.sha256(f"{ticker}:{verify_url}".encode()).hexdigest()

    existing_doc = conn.execute("SELECT document_id FROM documents WHERE sha256 = ?", (doc_sha,)).fetchone()
    if existing_doc:
        doc_id = existing_doc["document_id"]
        # Reset parse_status so we can re-run
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
               VALUES (?, ?, ?, 'verify', ?, ?, 'resource_update', ?, 'classified')""",
            (ticker, verify_url, doc_sha, announcement_date, now,
             f"JORC verification: {pdf_path.name}"),
        )
        doc_id = cursor.lastrowid
        conn.commit()
        print(f"Inserted document {doc_id}")

    conn.close()

    # Run the extraction
    from pipeline.orchestrator import _extract_resource_update
    stats = {"extracted": 0, "skipped": 0, "failed": 0}
    _extract_resource_update(doc_id, pdf_bytes, ticker, announcement_date, stats)
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
        print(f"  [{p['project_id']}] {p['project_name']} | stage={p['stage']} | "
              f"state={p['state']} | source={p.get('source', 'N/A')}")

    print(f"\n=== Project commodities ===")
    for p in projects:
        comms = conn.execute(
            "SELECT * FROM project_commodities WHERE project_id = ?",
            (p["project_id"],),
        ).fetchall()
        for c in comms:
            print(f"  project={p['project_name']} | {c['commodity']} | primary={c['is_primary']}")

    print(f"\n=== Resources ===")
    for p in projects:
        resources = conn.execute(
            "SELECT * FROM resources WHERE project_id = ?",
            (p["project_id"],),
        ).fetchall()
        for r in resources:
            print(f"  {r['category']:12s} | {r['tonnes']:.2f} Mt | "
                  f"{r['grade']:.2f} {r['grade_unit']} | "
                  f"{r['contained_metal']} {r['contained_metal_unit']} | "
                  f"date={r['effective_date']}")

    resource_count = conn.execute(
        "SELECT COUNT(*) FROM resources WHERE project_id IN (SELECT project_id FROM projects WHERE company_id = ?)",
        (company_id,),
    ).fetchone()[0]

    conn.close()

    if doc["parse_status"] == "parsed" and resource_count > 0:
        print(f"\nSUCCESS: {resource_count} resource rows extracted")
        sys.exit(0)
    else:
        print(f"\nFAILED: parse_status={doc['parse_status']}, resource_count={resource_count}")
        sys.exit(1)


if __name__ == "__main__":
    main()
