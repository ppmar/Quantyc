"""
ASX Announcements Poller

Pulls announcement metadata from the ASX MarkitDigital API, identifies
Appendix 5B filings by headline, downloads their PDF bytes, and runs
extraction immediately.

Two-step API:
    1. /search/predictive  → get entityXid for a ticker
    2. /markets/announcements?entityXids[]=...&itemsPerPage=N  → get announcements

Usage:
    python -m ingest.asx_poller --tickers DEG,RMS --count 100
    python -m ingest.asx_poller --file pilot_tickers.txt
"""

import argparse
import io
import logging
import time

import requests

from config import FETCH_DELAY, USER_AGENT
from db import get_connection, init_db
from ingest.document_store import store_document
from pipeline.classify import classify_headline

logger = logging.getLogger(__name__)

ASX_SEARCH_URL = "https://asx.api.markitdigital.com/asx-research/1.0/search/predictive"
ASX_ANNOUNCEMENTS_URL = "https://asx.api.markitdigital.com/asx-research/1.0/markets/announcements"
ASX_CDN_BASE = "https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0/file/"

HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}
PDF_HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/pdf"}

# Download and parse these doc types directly
TARGET_DOC_TYPES = {"appendix_5b"}
# Also download these to scan for embedded 5B forms
SCAN_DOC_TYPES = {"quarterly_activity"}


def _get_entity_xid(ticker: str) -> int | None:
    """Resolve a ticker to its ASX entityXid."""
    try:
        resp = requests.get(
            ASX_SEARCH_URL,
            params={"searchText": ticker.upper()},
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        items = resp.json().get("data", {}).get("items", [])
        if not items:
            logger.warning("No entity found for ticker %s", ticker)
            return None
        # Match exact ticker
        for item in items:
            if item.get("symbol", "").upper() == ticker.upper():
                return item.get("xidEntity")
        # Fallback to first result
        return items[0].get("xidEntity")
    except requests.RequestException as e:
        logger.error("Failed to resolve XID for %s: %s", ticker, e)
        return None


def fetch_announcements(ticker: str, xid: int, count: int = 100) -> list[dict]:
    """Fetch announcement metadata using the markets/announcements endpoint.

    Returns list of dicts with keys: header, url, document_date.
    """
    try:
        resp = requests.get(
            ASX_ANNOUNCEMENTS_URL,
            params={
                "entityXids[]": xid,
                "page": 0,
                "itemsPerPage": count,
            },
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", {}).get("items", [])

        results = []
        for item in items:
            doc_key = item.get("documentKey", "")
            results.append({
                "header": item.get("headline", ""),
                "url": ASX_CDN_BASE + doc_key if doc_key else "",
                "document_date": (item.get("date") or "")[:10],
            })
        return results
    except requests.RequestException as e:
        logger.error("Failed to fetch announcements for %s (xid=%s): %s", ticker, xid, e)
        return []


def fetch_pdf_bytes(url: str) -> bytes | None:
    """Download PDF into RAM. Returns bytes or None on failure."""
    try:
        resp = requests.get(url, headers=PDF_HEADERS, timeout=60)
        resp.raise_for_status()
        if len(resp.content) < 500:
            logger.warning("PDF too small (%d bytes): %s", len(resp.content), url)
            return None
        return resp.content
    except requests.RequestException as e:
        logger.error("Failed to download PDF %s: %s", url, e)
        return None


def poll_ticker(ticker: str, count: int = 100, status: dict | None = None) -> dict:
    """
    Poll ASX for a single ticker.
    1. Resolve ticker → entityXid
    2. Fetch up to `count` announcements
    3. Classify each by headline
    4. Only download + extract Appendix 5B filings
    """
    ticker = ticker.upper().strip()
    stats = {"new": 0, "skipped": 0, "failed": 0, "not_5b": 0}

    # Step 1: resolve XID
    xid = _get_entity_xid(ticker)
    if not xid:
        logger.warning("Could not resolve XID for %s, skipping", ticker)
        return stats

    time.sleep(FETCH_DELAY)

    # Step 2: fetch announcements
    announcements = fetch_announcements(ticker, xid, count)
    if not announcements:
        logger.warning("No announcements returned for %s", ticker)
        return stats

    if status:
        status["docs_total"] = status.get("docs_total", 0) + len(announcements)

    for ann in announcements:
        url = ann.get("url")
        if not url:
            continue

        header = ann.get("header", "")
        ann_date = ann.get("document_date")

        # Step 3: classify by headline — only proceed if it's a target or scan type
        doc_type = classify_headline(header)
        if doc_type not in TARGET_DOC_TYPES and doc_type not in SCAN_DOC_TYPES:
            stats["not_5b"] += 1
            if status:
                status["docs_done"] = status.get("docs_done", 0) + 1
            continue

        # Step 4: store document record (dedup by sha256)
        doc_id, is_new = store_document(
            ticker=ticker,
            url=url,
            source="asx_api",
            announcement_date=ann_date,
            header=header,
            doc_type=doc_type,
        )

        if not is_new:
            stats["skipped"] += 1
            if status:
                status["docs_done"] = status.get("docs_done", 0) + 1
            continue

        # Step 5: download PDF
        pdf_bytes = fetch_pdf_bytes(url)
        if not pdf_bytes:
            stats["failed"] += 1
            conn = get_connection()
            conn.execute(
                "UPDATE documents SET parse_status = 'failed', parse_error = 'download_failed' WHERE document_id = ?",
                (doc_id,),
            )
            conn.commit()
            conn.close()
            if status:
                status["docs_done"] = status.get("docs_done", 0) + 1
                status["failed_count"] = status.get("failed_count", 0) + 1
            time.sleep(FETCH_DELAY)
            continue

        # Step 6: extract (direct 5B or scan quarterly report for embedded 5B)
        _classify_and_extract(doc_id, doc_type, pdf_bytes)
        del pdf_bytes

        stats["new"] += 1
        if status:
            status["docs_done"] = status.get("docs_done", 0) + 1
            status["current_doc"] = header or str(doc_id)

        time.sleep(FETCH_DELAY)

    logger.info(
        "Poll %s: new=%d, skipped=%d, failed=%d, not_5b=%d",
        ticker, stats["new"], stats["skipped"], stats["failed"], stats["not_5b"],
    )
    return stats


def _classify_and_extract(doc_id: int, doc_type: str, pdf_bytes: bytes) -> None:
    """Classify, extract, and normalize a document immediately."""
    from pipeline.classify import contains_standardized_form
    from pipeline.extractors.appendix_5b import extract_appendix_5b
    from pipeline.normalize.company_financials import normalize_from_5b

    conn = get_connection()
    conn.execute(
        "UPDATE documents SET doc_type = ?, parse_status = 'classified' WHERE document_id = ?",
        (doc_type, doc_id),
    )
    conn.commit()
    conn.close()

    if doc_type == "appendix_5b":
        # Headline says 5B — extract directly
        result = extract_appendix_5b(doc_id, pdf_bytes)
        if result:
            normalize_from_5b(doc_id)
            _mark_status(doc_id, "parsed")
        else:
            _mark_status(doc_id, "failed", "extraction_empty")

    elif doc_type in SCAN_DOC_TYPES:
        # Quarterly activity report — scan for embedded 5B
        found_type = contains_standardized_form(pdf_bytes)
        if found_type == "appendix_5b":
            logger.info("Doc %d: found embedded 5B in quarterly report", doc_id)
            # Reclassify as appendix_5b
            conn = get_connection()
            conn.execute(
                "UPDATE documents SET doc_type = 'appendix_5b' WHERE document_id = ?",
                (doc_id,),
            )
            conn.commit()
            conn.close()
            result = extract_appendix_5b(doc_id, pdf_bytes)
            if result:
                normalize_from_5b(doc_id)
                _mark_status(doc_id, "parsed")
            else:
                _mark_status(doc_id, "failed", "extraction_empty")
        else:
            # No embedded 5B found — skip
            _mark_status(doc_id, "skipped")


def _mark_status(doc_id: int, status: str, error: str | None = None) -> None:
    conn = get_connection()
    if error:
        conn.execute(
            "UPDATE documents SET parse_status = ?, parse_error = ? WHERE document_id = ?",
            (status, error, doc_id),
        )
    else:
        conn.execute(
            "UPDATE documents SET parse_status = ? WHERE document_id = ?",
            (status, doc_id),
        )
    conn.commit()
    conn.close()


def poll_tickers(tickers: list[str], count: int = 100, status: dict | None = None) -> dict:
    """Poll multiple tickers sequentially. Returns combined stats."""
    combined = {"new": 0, "skipped": 0, "failed": 0, "not_5b": 0}
    for ticker in tickers:
        if status:
            status["ticker"] = ticker.upper()
        result = poll_ticker(ticker, count=count, status=status)
        for k in combined:
            combined[k] += result[k]
    return combined


def main():
    parser = argparse.ArgumentParser(description="Poll ASX announcements")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--tickers", type=str, help="Comma-separated tickers")
    group.add_argument("--file", type=str, help="File with one ticker per line")
    parser.add_argument("--count", type=int, default=100, help="Announcements per ticker")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    else:
        with open(args.file) as f:
            tickers = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    results = poll_tickers(tickers, count=args.count)
    print(f"\nPoll complete: {results}")


if __name__ == "__main__":
    main()
