#!/usr/bin/env python3
"""
One-off: populate Appendix 5B "Receipts from customers" (line 1.1) across the
existing universe, then run the deterministic production floor. REGEX ONLY — no
Gemini.

5B PDFs are stateless (never stored), so this re-fetches each company's latest
5B from the ASX CDN, re-parses it with the current extractor, writes
receipts_from_customers onto that filing's company_financials row, then sweeps
the production floor over every project. New filings get receipts automatically
via the normal ingest path; this is the backfill for what's already on file.

Usage:
    python -m scripts.backfill_receipts                 # whole universe
    python -m scripts.backfill_receipts --tickers MEK,CNB
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import FETCH_DELAY
from db import get_connection
from ingest.asx_poller import fetch_pdf_bytes
from pipeline.extractors.appendix_5b import _extract_all_fields, finalize_5b_amounts
from scripts.backfill_project_stages import apply_production_floors


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill 5B receipts(1.1) + run production floor")
    parser.add_argument("--tickers", help="Comma-separated tickers (default: all with a 5B)")
    args = parser.parse_args()

    conn = get_connection()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = [
            r["ticker"]
            for r in conn.execute(
                "SELECT DISTINCT ticker FROM documents WHERE doc_type='appendix_5b' AND url != ''"
            ).fetchall()
        ]

    fetched = updated = material = 0
    for t in tickers:
        doc = conn.execute(
            "SELECT document_id, url FROM documents "
            "WHERE ticker=? AND doc_type='appendix_5b' AND url != '' "
            "ORDER BY announcement_date DESC LIMIT 1",
            (t,),
        ).fetchone()
        if not doc:
            continue
        pdf = fetch_pdf_bytes(doc["url"])
        time.sleep(FETCH_DELAY)
        if not pdf:
            continue
        fetched += 1
        try:
            fin = finalize_5b_amounts(_extract_all_fields(pdf))
        except Exception as e:
            print(f"  {t}: parse error {type(e).__name__}")
            continue
        rec = fin.get("receipts_from_customers")
        if rec is None:
            continue
        cur = conn.execute(
            "UPDATE company_financials SET receipts_from_customers=? WHERE document_id=?",
            (rec, doc["document_id"]),
        )
        if cur.rowcount:
            updated += 1
        if rec >= 1_000_000:
            material += 1
            print(f"  {t}: receipts(1.1)={rec:,.0f} -> producing")
    conn.commit()

    sweep = apply_production_floors(conn)
    conn.close()
    print(
        f"\nfetched={fetched} receipts_updated={updated} material_receipts={material} "
        f"production_promoted={sweep['promoted']} scanned={sweep['scanned']}"
    )


if __name__ == "__main__":
    main()
