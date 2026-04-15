"""
ASX Announcements Poller

Pulls announcement metadata from the ASX MarkitDigital API, downloads
PDF bytes into RAM, and stores documents via document_store.

Usage:
    python -m ingest.asx_poller --tickers DEG,RMS --count 20
    python -m ingest.asx_poller --file pilot_tickers.txt
"""

import argparse
import io
import logging
import time

import requests

from config import ASX_API_URL, ASX_CDN_BASE, FETCH_DELAY, USER_AGENT
from db import get_connection, init_db
from ingest.document_store import store_document

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}
PDF_HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/pdf"}


def fetch_announcements(ticker: str, count: int = 20) -> list[dict]:
    """Fetch announcement metadata from ASX API.

    Returns list of dicts with keys: header, url, document_date.
    """
    url = ASX_API_URL.format(ticker=ticker.upper())
    params = {"count": count}
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
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
        logger.error("Failed to fetch announcements for %s: %s", ticker, e)
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


def poll_ticker(ticker: str, count: int = 20, status: dict | None = None) -> dict:
    """
    Poll ASX for a single ticker. Store new documents, return stats.
    PDF bytes are held in RAM only.
    """
    ticker = ticker.upper().strip()
    stats = {"new": 0, "skipped": 0, "failed": 0}

    announcements = fetch_announcements(ticker, count)
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

        # Try to store — returns (document_id, is_new)
        doc_id, is_new = store_document(
            ticker=ticker,
            url=url,
            source="asx_api",
            announcement_date=ann_date,
            header=header,
        )

        if not is_new:
            stats["skipped"] += 1
            if status:
                status["docs_done"] = status.get("docs_done", 0) + 1
            continue

        # Download PDF bytes
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

        stats["new"] += 1

        # Store pdf_bytes transiently for the orchestrator to pick up
        # For now we just mark as pending — orchestrator will re-download
        # In the future, pass bytes directly to orchestrator
        del pdf_bytes

        if status:
            status["docs_done"] = status.get("docs_done", 0) + 1
            status["current_doc"] = header or str(doc_id)

        time.sleep(FETCH_DELAY)

    logger.info("Poll %s: new=%d, skipped=%d, failed=%d", ticker, stats["new"], stats["skipped"], stats["failed"])
    return stats


def poll_tickers(tickers: list[str], count: int = 20, status: dict | None = None) -> dict:
    """Poll multiple tickers sequentially. Returns combined stats."""
    combined = {"new": 0, "skipped": 0, "failed": 0}
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
    parser.add_argument("--count", type=int, default=20, help="Announcements per ticker")
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
