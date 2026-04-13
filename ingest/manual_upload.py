"""
Manual Upload Handler

Processes PDF files uploaded via /api/upload. Registers them through
document_store, then hands off to the orchestrator.
"""

import hashlib
import io
import logging

from ingest.document_store import store_document

logger = logging.getLogger(__name__)


def handle_upload(ticker: str, filename: str, pdf_bytes: bytes, doc_type: str | None = None) -> tuple[int, bool]:
    """
    Register an uploaded PDF. Returns (document_id, is_new).

    The URL for manual uploads is a synthetic path derived from the filename.
    PDF bytes are NOT written to disk.
    """
    # Synthetic URL for dedup — manual uploads use filename as identifier
    url = f"upload://{ticker}/{filename}"

    doc_id, is_new = store_document(
        ticker=ticker,
        url=url,
        source="manual_upload",
        header=filename.replace(".pdf", "").replace("-", " ").replace("_", " "),
        doc_type=doc_type,
    )

    return doc_id, is_new
