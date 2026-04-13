"""
Document Store

Single point of entry for registering documents in the DB.
Handles sha256(ticker:url) dedup. Both asx_poller and manual_upload call this.
"""

import hashlib
import logging
from datetime import datetime, timezone

from db import get_connection

logger = logging.getLogger(__name__)


def compute_sha256(ticker: str, url: str) -> str:
    """Compute the dedup key: sha256 of 'TICKER:url'."""
    return hashlib.sha256(f"{ticker.upper()}:{url}".encode()).hexdigest()


def store_document(
    ticker: str,
    url: str,
    source: str,
    announcement_date: str | None = None,
    header: str | None = None,
    doc_type: str | None = None,
) -> tuple[int, bool]:
    """
    Register a document. Returns (document_id, is_new).

    If a document with the same sha256 already exists, returns the existing
    document_id and is_new=False.
    """
    ticker = ticker.upper().strip()
    sha = compute_sha256(ticker, url)
    now = datetime.now(timezone.utc).isoformat()

    conn = get_connection()

    # Check for existing
    existing = conn.execute(
        "SELECT document_id FROM documents WHERE sha256 = ?", (sha,)
    ).fetchone()

    if existing:
        conn.close()
        return existing["document_id"], False

    # Ensure company row exists
    conn.execute(
        "INSERT OR IGNORE INTO companies (ticker, first_seen_at, last_updated_at) VALUES (?, ?, ?)",
        (ticker, now, now),
    )

    # Insert new document
    cursor = conn.execute(
        """INSERT INTO documents
           (ticker, url, sha256, source, announcement_date, ingested_at,
            doc_type, header, parse_status, local_path)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', '')""",
        (ticker, url, sha, source, announcement_date, now, doc_type, header),
    )
    doc_id = cursor.lastrowid

    conn.commit()
    conn.close()

    logger.info("Stored document %d for %s (sha=%s…)", doc_id, ticker, sha[:12])
    return doc_id, True
