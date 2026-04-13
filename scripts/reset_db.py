#!/usr/bin/env python3
"""
Database Reset Script

Backs up existing document metadata, drops all tables, and recreates
with the lean schema from db/schema.sql.

Usage:
    python scripts/reset_db.py              # backup + reset
    python scripts/reset_db.py --dry-run    # print plan without executing
    python scripts/reset_db.py --no-backup  # full clean wipe, no backup
"""

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "db" / "quantyc.db"
SCHEMA_PATH = ROOT / "db" / "schema.sql"
BACKUP_PATH = ROOT / "db" / "_documents_backup.json"


def _get_existing_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return [r[0] for r in rows]


def _backup_documents(conn: sqlite3.Connection) -> list[dict]:
    """Extract document metadata for re-import after schema change."""
    try:
        rows = conn.execute(
            "SELECT * FROM documents"
        ).fetchall()
    except sqlite3.OperationalError:
        logger.info("No documents table found — nothing to back up")
        return []

    columns = [desc[0] for desc in conn.execute("SELECT * FROM documents LIMIT 0").description]
    docs = [dict(zip(columns, row)) for row in rows]
    return docs


def _write_backup(docs: list[dict]) -> None:
    BACKUP_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(BACKUP_PATH, "w") as f:
        json.dump(docs, f, indent=2, default=str)
    logger.info("Backed up %d documents to %s", len(docs), BACKUP_PATH)


def _reimport_documents(conn: sqlite3.Connection, docs: list[dict]) -> int:
    """Re-import backed-up documents into the new schema."""
    imported = 0
    now = datetime.now(timezone.utc).isoformat()

    for doc in docs:
        # Map old fields to new fields
        ticker = doc.get("company_ticker") or doc.get("ticker") or ""
        url = doc.get("url") or ""
        if not ticker or not url:
            continue

        import hashlib
        sha = hashlib.sha256(f"{ticker}:{url}".encode()).hexdigest()

        ann_date = doc.get("announcement_date")
        source = "asx_api" if url.startswith("http") else "manual_upload"

        try:
            conn.execute(
                """INSERT OR IGNORE INTO documents
                   (ticker, url, sha256, source, announcement_date, ingested_at,
                    doc_type, parse_status, local_path)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', '')""",
                (ticker, url, sha, source, ann_date, now, doc.get("doc_type")),
            )
            imported += 1
        except sqlite3.IntegrityError:
            pass  # sha256 conflict — already imported

    conn.commit()
    return imported


def reset_db(dry_run: bool = False, no_backup: bool = False) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    if not SCHEMA_PATH.exists():
        logger.error("Schema file not found: %s", SCHEMA_PATH)
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    existing_tables = _get_existing_tables(conn)

    logger.info("Database: %s", DB_PATH)
    logger.info("Existing tables: %s", existing_tables or "(none)")

    # Step 1: Backup
    backed_up_docs = []
    if not no_backup and existing_tables:
        backed_up_docs = _backup_documents(conn)
        if dry_run:
            logger.info("[DRY RUN] Would back up %d documents to %s", len(backed_up_docs), BACKUP_PATH)
        else:
            if backed_up_docs:
                _write_backup(backed_up_docs)

    # Step 2: Drop all tables
    if dry_run:
        logger.info("[DRY RUN] Would drop tables: %s", existing_tables)
    else:
        for table in existing_tables:
            conn.execute(f"DROP TABLE IF EXISTS [{table}]")
            logger.info("Dropped table: %s", table)
        conn.commit()

    # Step 3: Create new schema
    schema_sql = SCHEMA_PATH.read_text()
    if dry_run:
        logger.info("[DRY RUN] Would execute schema from %s", SCHEMA_PATH)
    else:
        conn.executescript(schema_sql)
        logger.info("Created new schema from %s", SCHEMA_PATH)

    # Step 4: Re-import documents
    if not no_backup and backed_up_docs:
        if dry_run:
            logger.info("[DRY RUN] Would re-import %d documents with parse_status='pending'", len(backed_up_docs))
        else:
            imported = _reimport_documents(conn, backed_up_docs)
            logger.info("Re-imported %d / %d documents", imported, len(backed_up_docs))

    # Step 5: Verify
    if not dry_run:
        new_tables = _get_existing_tables(conn)
        logger.info("New tables: %s", new_tables)
        for table in new_tables:
            count = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
            logger.info("  %s: %d rows", table, count)

    conn.close()
    logger.info("Done%s.", " (dry run)" if dry_run else "")


def main():
    parser = argparse.ArgumentParser(description="Reset database with lean schema")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without executing")
    parser.add_argument("--no-backup", action="store_true", help="Full clean wipe, no backup")
    args = parser.parse_args()

    reset_db(dry_run=args.dry_run, no_backup=args.no_backup)


if __name__ == "__main__":
    main()
