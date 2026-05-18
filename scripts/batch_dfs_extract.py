#!/usr/bin/env python3
"""
Batch DFS extraction — fetch announcements, find DFS docs, extract + revalue.
Only downloads DFS-classified PDFs to save time and API calls.

Usage:
    GOOGLE_API_KEY=... python scripts/batch_dfs_extract.py
"""
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from db import init_db, get_connection
from ingest.asx_poller import (
    _get_entity_xid, fetch_announcements, fetch_pdf_bytes, _extract_dfs,
    FETCH_DELAY,
)
from ingest.document_store import store_document
from pipeline.classify import classify_headline

TICKERS = [
    # Producers
    "WAF", "CMM", "RMS", "RRL", "GMD", "PRU",
    # Development / recent DFS
    "DEG", "BGL", "MEK", "SPR", "MTH", "TRM", "ASM",
    # Advanced explorers
    "ARV", "NST",
]

SCAN_COUNT = 1000


def find_and_extract_dfs(ticker: str) -> dict:
    """Fetch announcements, find DFS headlines, download + extract."""
    ticker = ticker.upper().strip()
    stats = {"dfs_found": 0, "dfs_extracted": 0, "dfs_failed": 0}

    xid = _get_entity_xid(ticker)
    if not xid:
        logger.warning("[%s] Could not resolve XID, skipping", ticker)
        return stats

    time.sleep(FETCH_DELAY)

    anns = fetch_announcements(ticker, xid, SCAN_COUNT)
    if not anns:
        logger.warning("[%s] No announcements", ticker)
        return stats

    # Find DFS-classified announcements
    dfs_anns = []
    for ann in anns:
        header = ann.get("header", "")
        doc_type = classify_headline(header)
        if doc_type == "study_dfs":
            dfs_anns.append(ann)

    logger.info("[%s] %d announcements scanned, %d DFS found", ticker, len(anns), len(dfs_anns))

    if not dfs_anns:
        # Also check for study_pfs / study_scoping as info
        studies = [(a["document_date"], classify_headline(a["header"]), a["header"][:80])
                   for a in anns if classify_headline(a.get("header", "")) and "study" in (classify_headline(a.get("header", "")) or "")]
        if studies:
            logger.info("[%s] Non-DFS studies found: %s", ticker, studies)
        return stats

    for ann in dfs_anns:
        url = ann.get("url", "")
        header = ann.get("header", "")
        ann_date = ann.get("document_date")

        stats["dfs_found"] += 1
        logger.info("[%s] DFS: %s (%s)", ticker, header[:80], ann_date)

        # Store doc record
        doc_id, is_new = store_document(
            ticker=ticker, url=url, source="asx_api",
            announcement_date=ann_date, header=header, doc_type="study_dfs",
        )

        # Check if already parsed
        conn = get_connection()
        row = conn.execute("SELECT parse_status FROM documents WHERE document_id = ?", (doc_id,)).fetchone()
        conn.close()
        if row and row["parse_status"] == "parsed":
            logger.info("[%s] Doc %d already parsed, skipping", ticker, doc_id)
            stats["dfs_extracted"] += 1
            continue

        # Download PDF
        pdf_bytes = fetch_pdf_bytes(url)
        if not pdf_bytes:
            logger.warning("[%s] Failed to download PDF: %s", ticker, url)
            stats["dfs_failed"] += 1
            time.sleep(FETCH_DELAY)
            continue

        logger.info("[%s] Downloaded %d bytes, extracting...", ticker, len(pdf_bytes))

        try:
            _extract_dfs(doc_id, pdf_bytes)
            stats["dfs_extracted"] += 1
        except Exception as e:
            logger.error("[%s] Extraction failed: %s", ticker, e)
            stats["dfs_failed"] += 1

        del pdf_bytes
        time.sleep(FETCH_DELAY)

    return stats


def main():
    init_db()

    # Apply migrations
    conn = get_connection()
    from pathlib import Path
    migrations_dir = Path(__file__).resolve().parent.parent / "db" / "migrations"
    if migrations_dir.exists():
        for m in sorted(migrations_dir.glob("*.sql")):
            try:
                conn.executescript(m.read_text())
            except Exception:
                pass
    conn.close()

    results = {}
    for ticker in TICKERS:
        logger.info("=" * 60)
        logger.info("Processing %s", ticker)
        logger.info("=" * 60)
        try:
            stats = find_and_extract_dfs(ticker)
            results[ticker] = stats
        except Exception as e:
            logger.error("[%s] Unexpected error: %s", ticker, e)
            results[ticker] = {"error": str(e)}

    # Summary
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    for ticker, stats in results.items():
        logger.info("  %s: %s", ticker, stats)


if __name__ == "__main__":
    main()
