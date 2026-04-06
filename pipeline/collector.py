"""
ASX Announcements Collector

Downloads PDFs from the ASX announcements API and records metadata
in the documents table.

Usage:
    python -m pipeline.collector --tickers DEG,RMS --count 20
    python -m pipeline.collector --file pilot_tickers.txt
"""

import argparse
import hashlib
import logging
import time
from pathlib import Path

import requests

from db import get_connection, init_db
from pipeline.classifier import classify_title

logger = logging.getLogger(__name__)

ASX_API_URL = "https://www.asx.com.au/asx/1/company/{ticker}/announcements"
ASX_BASE_URL = "https://www.asx.com.au"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; QuantycResearch/1.0)",
    "Accept": "application/json",
}
PDF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; QuantycResearch/1.0)",
    "Accept": "application/pdf",
}

DELAY_SECONDS = 1.5


def fetch_announcements(ticker: str, count: int = 20) -> list[dict]:
    """Fetch announcement metadata from ASX API for a given ticker."""
    url = ASX_API_URL.format(ticker=ticker.upper())
    params = {"count": count, "market_sensitive": "false"}
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])
    except requests.RequestException as e:
        logger.error("Failed to fetch announcements for %s: %s", ticker, e)
        return []


def doc_id_from_url(url: str) -> str:
    """Generate a deterministic document ID from the URL."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def download_pdf(url: str, local_path: Path) -> bool:
    """Download a PDF from the given URL to local_path."""
    try:
        resp = requests.get(url, headers=PDF_HEADERS, timeout=60, stream=True)
        resp.raise_for_status()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except requests.RequestException as e:
        logger.error("Failed to download %s: %s", url, e)
        return False


def collect_ticker(ticker: str, count: int = 20, skip_existing: bool = True) -> int:
    """
    Collect announcements for a single ticker.
    Returns number of new documents collected.
    """
    ticker = ticker.upper().strip()
    logger.info("Collecting announcements for %s (count=%d)", ticker, count)

    announcements = fetch_announcements(ticker, count)
    if not announcements:
        logger.warning("No announcements found for %s", ticker)
        return 0

    conn = get_connection()
    new_count = 0

    for ann in announcements:
        pdf_path = ann.get("url")
        if not pdf_path:
            continue

        full_url = ASX_BASE_URL + pdf_path if pdf_path.startswith("/") else pdf_path
        doc_id = doc_id_from_url(full_url)

        # Skip if already collected
        if skip_existing:
            row = conn.execute("SELECT id FROM documents WHERE id = ?", (doc_id,)).fetchone()
            if row:
                continue

        # Build local path: data/raw/{TICKER}/{doc_id}.pdf
        ticker_dir = RAW_DIR / ticker
        local_path = ticker_dir / f"{doc_id}.pdf"

        # Extract announcement date and header
        ann_date = ann.get("document_date")
        header = ann.get("header", "")
        doc_type = classify_title(header)

        # Download the PDF
        logger.info("Downloading: %s", header or full_url)
        if download_pdf(full_url, local_path):
            conn.execute(
                """INSERT OR IGNORE INTO documents
                   (id, company_ticker, doc_type, header, announcement_date, url, local_path, parse_status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')""",
                (doc_id, ticker, doc_type, header, ann_date, full_url, str(local_path)),
            )
            new_count += 1
        else:
            logger.warning("Skipping %s — download failed", doc_id)

        time.sleep(DELAY_SECONDS)

    conn.commit()
    conn.close()
    logger.info("Collected %d new documents for %s", new_count, ticker)
    return new_count


def collect_all(tickers: list[str], count: int = 20) -> dict[str, int]:
    """Collect announcements for a list of tickers."""
    results = {}
    for ticker in tickers:
        results[ticker] = collect_ticker(ticker, count)
        time.sleep(DELAY_SECONDS)
    return results


def main():
    parser = argparse.ArgumentParser(description="Collect ASX announcements")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--tickers", type=str, help="Comma-separated tickers (e.g. DEG,RMS)")
    group.add_argument("--file", type=str, help="Path to file with one ticker per line")
    parser.add_argument("--count", type=int, default=20, help="Announcements per ticker (default: 20)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    init_db()

    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    else:
        with open(args.file) as f:
            tickers = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    results = collect_all(tickers, count=args.count)

    total = sum(results.values())
    print(f"\nCollected {total} new documents across {len(results)} tickers:")
    for ticker, n in results.items():
        print(f"  {ticker}: {n}")


if __name__ == "__main__":
    main()
