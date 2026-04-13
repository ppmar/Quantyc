"""
Upload endpoint — POST /api/upload

Accepts multipart PDF uploads, registers via document_store,
and kicks off the orchestrator.
"""

import io
import logging

from flask import Blueprint, jsonify, request

from ingest.manual_upload import handle_upload
from pipeline.classify import classify

logger = logging.getLogger(__name__)

bp = Blueprint("upload", __name__)


@bp.route("/api/upload", methods=["POST"])
def api_upload():
    """
    Upload PDFs for a ticker. Accepts multipart form data with:
    - ticker: company ticker (required)
    - doc_type: override document type (optional)
    - files: one or more PDF files
    """
    ticker = request.form.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker is required"}), 400

    override_doc_type = request.form.get("doc_type", "").strip() or None

    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files provided"}), 400

    uploaded = []
    skipped = []
    for f in files:
        if not f.filename or not f.filename.lower().endswith(".pdf"):
            continue

        filename = f.filename
        pdf_bytes = f.read()

        # Classify if no override
        doc_type = override_doc_type
        if not doc_type:
            doc_type = classify(headline=filename, pdf_bytes=pdf_bytes)

        doc_id, is_new = handle_upload(
            ticker=ticker,
            filename=filename,
            pdf_bytes=pdf_bytes,
            doc_type=doc_type,
        )

        if not is_new:
            skipped.append({"filename": filename, "document_id": doc_id})
            continue

        # Update doc_type and advance to classified
        from db import get_connection
        conn = get_connection()
        conn.execute(
            "UPDATE documents SET doc_type = ?, parse_status = 'classified' WHERE document_id = ?",
            (doc_type, doc_id),
        )
        conn.commit()
        conn.close()

        uploaded.append({"filename": filename, "document_id": doc_id, "doc_type": doc_type})
        del pdf_bytes

    if not uploaded and not skipped:
        return jsonify({"error": "No valid PDF files"}), 400

    if not uploaded and skipped:
        return jsonify({
            "status": "skipped",
            "message": f"All {len(skipped)} file(s) already uploaded",
            "duplicates": skipped,
        }), 409

    response = {"status": "uploaded", "ticker": ticker, "files": uploaded}
    if skipped:
        response["duplicates"] = skipped
    return jsonify(response)
