"""
ASX Stateless PDF Fetcher

Fetches announcements from the ASX API, downloads PDFs into RAM (never disk),
parses them via BytesIO, and writes results to SQLite.

Usage:
    from pipeline.asx_fetcher import ingest_ticker, ingest_tickers
"""

import io
import logging
import time

import requests

from db import get_connection
from pipeline.collector import (
    fetch_announcements,
    doc_id_from_url,
    ASX_BASE_URL,
    PDF_HEADERS,
)
from pipeline.classifier import classify_title
from pipeline.parsers.appendix_5b import parse_appendix_5b
from pipeline.parsers.resource import parse_resource
from pipeline.parsers.drill_results import parse_drill_results
from pipeline.parsers.capital_raise import parse_capital_raise
from pipeline.parsers.study import parse_study
from pipeline.parsers.generic import parse_generic

logger = logging.getLogger(__name__)

# Skip these — purely administrative filings with no extractable data
SKIP_KEYWORDS = [
    "cessation of securities", "change of director", "cleansing notice",
    "appendix 2a", "appendix 3b", "appendix 3y", "appendix 3z",
    "s708a notice", "statement of cdis", "becoming a substantial",
    "ceasing to be a substantial", "notification of buy-back",
    "dividend/distribution", "notification regarding unquoted",
    "daily share buy-back", "date of agm", "proxy form",
    "letter to shareholders",
]

PARSERS = {
    "appendix_5b":      parse_appendix_5b,
    "quarterly_report": parse_generic,  # LLM-based, handles all quarterly formats
    "resource_update":  parse_resource,
    "drill_results":    parse_drill_results,
    "capital_raise":    parse_capital_raise,
    "study":            parse_study,
    "annual_report":    parse_generic,
    "other":            parse_generic,
}

FETCH_DELAY = 0.5


def _should_skip(header: str) -> bool:
    """Check if an announcement is purely administrative and should be skipped."""
    h = header.lower()
    return any(kw in h for kw in SKIP_KEYWORDS)


def _fetch_pdf_bytes(url: str) -> bytes | None:
    """Download PDF into RAM. Returns bytes or None on failure."""
    try:
        resp = requests.get(url, headers=PDF_HEADERS, timeout=60)
        resp.raise_for_status()
        if len(resp.content) < 500:
            logger.warning("PDF too small (%d bytes), likely not a real PDF: %s", len(resp.content), url)
            return None
        return resp.content
    except requests.RequestException as e:
        logger.error("Failed to download PDF %s: %s", url, e)
        return None


def ingest_ticker(ticker: str, count: int = 50, status: dict | None = None) -> dict:
    """
    Fetch, filter, and parse announcements for a single ticker.
    PDFs are held in RAM only — never written to disk.

    Args:
        ticker: ASX ticker symbol
        count: Number of announcements to fetch from API
        status: Optional pipeline_status dict for live progress tracking

    Returns:
        {"fetched": N, "parsed": N, "skipped": N, "failed": N}
    """
    ticker = ticker.upper().strip()
    stats = {"fetched": 0, "parsed": 0, "skipped": 0, "failed": 0}

    announcements = fetch_announcements(ticker, count)
    if not announcements:
        logger.warning("No announcements returned for %s", ticker)
        return stats

    conn = get_connection()

    # Filter relevant and deduplicate
    to_process = []
    for ann in announcements:
        header = ann.get("header", "")
        full_url = ann.get("url")
        if not full_url or _should_skip(header):
            continue

        doc_id = doc_id_from_url(full_url)

        # Dedup against DB
        existing = conn.execute("SELECT id FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if existing:
            stats["skipped"] += 1
            continue

        to_process.append({
            "doc_id": doc_id,
            "url": full_url,
            "header": header,
            "date": ann.get("document_date"),
            "doc_type": classify_title(header),
        })

    conn.close()

    if status:
        status["docs_total"] = status.get("docs_total", 0) + len(to_process)

    for item in to_process:
        doc_id = item["doc_id"]
        doc_type = item["doc_type"]
        header = item["header"]

        if status:
            status["current_doc"] = header or doc_id

        # 1. Register document in DB first (with empty local_path)
        conn = get_connection()
        conn.execute(
            "INSERT OR IGNORE INTO companies (ticker, updated_at) VALUES (?, CURRENT_TIMESTAMP)",
            (ticker,),
        )
        ann_date = item["date"]
        synthetic_filename = f"{ann_date}_{doc_type}.pdf" if ann_date else f"{doc_id}.pdf"
        conn.execute(
            """INSERT OR IGNORE INTO documents
               (id, company_ticker, doc_type, header, original_filename,
                announcement_date, url, local_path, parse_status)
               VALUES (?, ?, ?, ?, ?, ?, ?, '', 'pending')""",
            (doc_id, ticker, doc_type, header, synthetic_filename, ann_date, item["url"]),
        )
        conn.commit()
        conn.close()

        # 2. Download PDF bytes into RAM
        pdf_bytes = _fetch_pdf_bytes(item["url"])
        if not pdf_bytes:
            stats["failed"] += 1
            conn = get_connection()
            conn.execute("UPDATE documents SET parse_status = 'failed' WHERE id = ?", (doc_id,))
            conn.commit()
            conn.close()
            if status:
                status["docs_done"] = status.get("docs_done", 0) + 1
                status["failed_count"] = status.get("failed_count", 0) + 1
            time.sleep(FETCH_DELAY)
            continue

        stats["fetched"] += 1

        # 3. Parse via BytesIO — never touches disk
        parser = PARSERS.get(doc_type)
        if not parser:
            logger.info("No parser for doc_type '%s', marking needs_review: %s", doc_type, doc_id)
            conn = get_connection()
            conn.execute("UPDATE documents SET parse_status = 'needs_review' WHERE id = ?", (doc_id,))
            conn.commit()
            conn.close()
            del pdf_bytes
            if status:
                status["docs_done"] = status.get("docs_done", 0) + 1
            time.sleep(FETCH_DELAY)
            continue

        bio = io.BytesIO(pdf_bytes)
        del pdf_bytes  # free raw bytes immediately

        try:
            result = parser(doc_id, pdf_source=bio)
            if result:
                stats["parsed"] += 1
            else:
                stats["failed"] += 1
                if status:
                    status["failed_count"] = status.get("failed_count", 0) + 1
        except Exception as e:
            logger.error("Parser error on %s: %s", doc_id, e)
            stats["failed"] += 1
            conn = get_connection()
            conn.execute("UPDATE documents SET parse_status = 'failed' WHERE id = ?", (doc_id,))
            conn.commit()
            conn.close()
            if status:
                status["failed_count"] = status.get("failed_count", 0) + 1
        finally:
            del bio  # free BytesIO

        if status:
            status["docs_done"] = status.get("docs_done", 0) + 1

        time.sleep(FETCH_DELAY)

    logger.info(
        "Ingest %s: fetched=%d, parsed=%d, skipped=%d, failed=%d",
        ticker, stats["fetched"], stats["parsed"], stats["skipped"], stats["failed"],
    )
    return stats


def ingest_tickers(tickers: list[str], count: int = 50, status: dict | None = None) -> dict:
    """
    Ingest announcements for multiple tickers sequentially.
    Returns combined stats.
    """
    combined = {"fetched": 0, "parsed": 0, "skipped": 0, "failed": 0}
    for ticker in tickers:
        if status:
            status["ticker"] = ticker.upper()
        result = ingest_ticker(ticker, count=count, status=status)
        for k in combined:
            combined[k] += result[k]
    return combined
