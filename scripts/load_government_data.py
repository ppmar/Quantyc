"""
Bootstrap projects table from OZMIN (national) and MINEDEX (WA).

Usage:
    python -m scripts.load_government_data --ozmin --minedex --dry-run
    python -m scripts.load_government_data --ozmin
    python -m scripts.load_government_data --minedex --csv data/minedex_extract.csv
"""

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Load government mining data into projects table")
    parser.add_argument("--ozmin", action="store_true", help="Load from OZMIN (Geoscience Australia)")
    parser.add_argument("--minedex", action="store_true", help="Load from MINEDEX (WA DMIRS)")
    parser.add_argument("--csv", type=str, default=None, help="Path to MINEDEX CSV extract")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be inserted, no DB writes")
    args = parser.parse_args()

    if not args.ozmin and not args.minedex:
        parser.error("Specify at least one of --ozmin or --minedex")

    from db import init_db
    init_db()

    total_matched = 0

    if args.ozmin:
        from ingest.ozmin_loader import load_ozmin
        logger.info("=== OZMIN ===")
        stats = load_ozmin(dry_run=args.dry_run)
        logger.info(
            "OZMIN: fetched %d features, matched %d to known tickers, "
            "inserted %d projects, updated %d, %d commodities, "
            "skipped %d (unmapped operator), skipped %d (no company row)",
            stats["fetched"], stats["matched"], stats["inserted"],
            stats.get("updated", 0), stats["commodities_inserted"],
            stats["skipped_unmapped"], stats["skipped_no_company"],
        )
        total_matched += stats["matched"]

    if args.minedex:
        from ingest.minedex_loader import load_minedex
        logger.info("=== MINEDEX ===")
        csv_path = args.csv or "data/minedex_extract.csv"
        stats = load_minedex(csv_path=csv_path, dry_run=args.dry_run)
        if "error" in stats:
            logger.error("MINEDEX failed: %s", stats["error"])
        else:
            logger.info(
                "MINEDEX: fetched %d records, matched %d to known tickers, "
                "inserted %d projects, updated %d, %d commodities, "
                "skipped %d (unmapped), skipped %d (no company), skipped %d (duplicate)",
                stats["fetched"], stats["matched"], stats["inserted"],
                stats.get("updated", 0), stats["commodities_inserted"],
                stats["skipped_unmapped"], stats["skipped_no_company"],
                stats.get("skipped_duplicate", 0),
            )
            total_matched += stats["matched"]

    if total_matched == 0:
        logger.error("No matches at all — check operator mapping CSV and companies table")
        sys.exit(1)

    logger.info("Done. Total matched: %d", total_matched)


if __name__ == "__main__":
    main()
