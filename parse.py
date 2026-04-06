"""
One-command parser — registers, classifies, parses, normalizes, loads,
and visualizes all PDFs found in data/raw/.

Usage:
    python parse.py                     # process all tickers in data/raw/
    python parse.py --ticker SXG        # process only SXG
    python parse.py --viz               # also generate visualizations
"""

import argparse
import hashlib
import logging
import sys
from pathlib import Path

from db import get_connection, init_db
from pipeline.classifier import classify_title
from pipeline.parsers.appendix_5b import parse_appendix_5b
from pipeline.parsers.drill_results import parse_drill_results
from pipeline.parsers.resource import parse_resource
from pipeline.parsers.capital_raise import parse_capital_raise
from pipeline.parsers.study import parse_study
from pipeline.normalizer import normalize_document_extractions
from pipeline.loader import load_document

logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).resolve().parent / "data" / "raw"

PARSERS = {
    "appendix_5b":      parse_appendix_5b,
    "quarterly_report": parse_appendix_5b,
    "resource_update":  parse_resource,
    "drill_results":    parse_drill_results,
    "capital_raise":    parse_capital_raise,
    "study":            parse_study,
}


def _classify_from_content(doc_id: str) -> str:
    """Try to classify a document by reading the first page of the PDF."""
    conn = get_connection()
    row = conn.execute("SELECT local_path FROM documents WHERE id = ?", (doc_id,)).fetchone()
    conn.close()
    if not row or not row["local_path"]:
        return "other"

    pdf_path = Path(__file__).resolve().parent / row["local_path"]
    if not pdf_path.exists():
        logger.warning("PDF not found for reclassification: %s", pdf_path)
        return "other"

    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            text = ""
            for page in pdf.pages[:3]:
                text += (page.extract_text() or "") + "\n"
        doc_type = classify_title(text)
        return doc_type
    except Exception as e:
        logger.warning("Could not read PDF for classification %s: %s", doc_id, e)
        return "other"


def register_pdfs(ticker: str | None = None) -> int:
    """Scan data/raw/ for unregistered PDFs and add them to the documents table."""
    conn = get_connection()
    registered = 0

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if ticker:
        dirs = [RAW_DIR / ticker.upper()]
    else:
        dirs = [d for d in RAW_DIR.iterdir() if d.is_dir()]

    for ticker_dir in dirs:
        tk = ticker_dir.name.upper()
        for pdf_path in sorted(ticker_dir.glob("*.pdf")):
            rel_path = str(pdf_path.relative_to(Path(__file__).resolve().parent))
            doc_id = hashlib.sha256(rel_path.encode()).hexdigest()[:16]

            existing = conn.execute("SELECT id FROM documents WHERE id = ?", (doc_id,)).fetchone()
            if existing:
                continue

            # Use filename as a fallback header
            header = pdf_path.stem.replace("-", " ").replace("_", " ")
            doc_type = classify_title(header)

            conn.execute(
                "INSERT OR IGNORE INTO companies (ticker, updated_at) VALUES (?, CURRENT_TIMESTAMP)",
                (tk,),
            )
            conn.execute(
                """INSERT OR IGNORE INTO documents
                   (id, company_ticker, doc_type, header, announcement_date, url, local_path, parse_status)
                   VALUES (?, ?, ?, ?, NULL, '', ?, 'pending')""",
                (doc_id, tk, doc_type, header, rel_path),
            )
            registered += 1
            logger.info("Registered: %s -> %s (%s)", pdf_path.name, doc_type, doc_id)

    conn.commit()
    conn.close()
    return registered


def run_pipeline(ticker: str | None = None) -> dict:
    """Run the full pipeline on all pending documents."""
    init_db()

    stats = {"registered": 0, "parsed": 0, "normalized": 0, "loaded": 0, "failed": 0, "skipped": 0}

    # 1. Register any new PDFs
    stats["registered"] = register_pdfs(ticker)

    # 2. Parse all pending documents
    conn = get_connection()
    where = "WHERE parse_status = 'pending'"
    params = ()
    if ticker:
        where += " AND company_ticker = ?"
        params = (ticker.upper(),)

    docs = conn.execute(f"SELECT id, doc_type, header FROM documents {where}", params).fetchall()
    conn.close()

    for doc in docs:
        doc_id = doc["id"]
        doc_type = doc["doc_type"]
        parser = PARSERS.get(doc_type)

        # Try to reclassify 'other' docs from PDF content
        if not parser and doc_type == "other":
            doc_type = _classify_from_content(doc_id)
            if doc_type != "other":
                conn2 = get_connection()
                conn2.execute("UPDATE documents SET doc_type = ? WHERE id = ?", (doc_type, doc_id))
                conn2.commit()
                conn2.close()
                parser = PARSERS.get(doc_type)
                logger.info("Reclassified %s as '%s' from PDF content", doc_id, doc_type)

        if not parser:
            logger.info("No parser for doc_type '%s' — marking needs_review %s", doc_type, doc_id)
            conn2 = get_connection()
            conn2.execute("UPDATE documents SET parse_status = 'needs_review' WHERE id = ?", (doc_id,))
            conn2.commit()
            conn2.close()
            stats["skipped"] += 1
            continue

        logger.info("Parsing [%s] %s: %s", doc_type, doc_id, doc["header"])
        try:
            result = parser(doc_id)
            if result:
                stats["parsed"] += 1
            else:
                stats["failed"] += 1
        except Exception as e:
            logger.error("Parser error on %s: %s", doc_id, e)
            stats["failed"] += 1

    # 3. Normalize
    conn = get_connection()
    parsed_docs = conn.execute(
        "SELECT DISTINCT document_id FROM staging_extractions WHERE normalized_value IS NULL"
    ).fetchall()
    conn.close()

    for row in parsed_docs:
        n = normalize_document_extractions(row["document_id"])
        if n > 0:
            stats["normalized"] += 1

    # 4. Load into core tables
    conn = get_connection()
    where = "WHERE parse_status = 'done'"
    params = ()
    if ticker:
        where += " AND company_ticker = ?"
        params = (ticker.upper(),)

    done_docs = conn.execute(f"SELECT id FROM documents {where}", params).fetchall()
    conn.close()

    for row in done_docs:
        try:
            if load_document(row["id"]):
                stats["loaded"] += 1
        except Exception as e:
            logger.error("Loader error on %s: %s", row["id"], e)

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="One-command pipeline: register, parse, normalize, load, visualize"
    )
    parser.add_argument("--ticker", type=str, help="Process only this ticker")
    parser.add_argument("--viz", action="store_true", help="Generate visualizations after parsing")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")

    ticker = args.ticker.upper() if args.ticker else None

    print(f"\n{'='*50}")
    print(f"  Pipeline Run{f' — {ticker}' if ticker else ' — All Tickers'}")
    print(f"{'='*50}\n")

    stats = run_pipeline(ticker)

    print(f"\n{'='*50}")
    print(f"  Results")
    print(f"{'='*50}")
    print(f"  New PDFs registered:  {stats['registered']}")
    print(f"  Parsed successfully:  {stats['parsed']}")
    print(f"  Parse failures:       {stats['failed']}")
    print(f"  Skipped (no parser):  {stats['skipped']}")
    print(f"  Normalized:           {stats['normalized']}")
    print(f"  Loaded to core DB:    {stats['loaded']}")
    print(f"{'='*50}\n")

    if args.viz:
        print("Generating visualizations...")
        from visualize import main as viz_main
        viz_main()

    # Show what's in the DB now
    conn = get_connection()
    for row in conn.execute("""
        SELECT company_ticker, doc_type, parse_status, COUNT(*) as n
        FROM documents GROUP BY company_ticker, doc_type, parse_status
        ORDER BY company_ticker, doc_type
    """).fetchall():
        print(f"  {row['company_ticker']:6s} {row['doc_type']:20s} {row['parse_status']:10s} x{row['n']}")
    conn.close()


if __name__ == "__main__":
    main()
