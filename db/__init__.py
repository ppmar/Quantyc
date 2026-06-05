import os
import sqlite3
from pathlib import Path

_volume = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
DB_PATH = Path(_volume) / "quantyc.db" if _volume else Path(__file__).resolve().parent / "quantyc.db"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"
MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())
    # Apply migrations
    if MIGRATIONS_DIR.exists():
        for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
            with open(migration) as f:
                try:
                    conn.executescript(f.read())
                except Exception:
                    pass  # migrations should be idempotent
    # Add columns to existing tables (safe: ignores if already present)
    _safe_add_columns(conn)
    conn.close()


def _safe_add_columns(conn):
    """Add columns that may be missing from older DBs."""
    additions = [
        ("resources", "grade_unit", "TEXT"),
        ("resources", "contained_metal_unit", "TEXT"),
        ("resources", "cutoff_grade", "REAL"),
        ("resources", "cutoff_grade_unit", "TEXT"),
        ("projects", "source", "TEXT"),
        ("resources", "section", "TEXT"),
        # 0007 migration's revaluations ALTER never lands on DBs where the
        # studies ALTER already ran (executescript aborts on duplicate column).
        ("revaluations", "study_confidence_tier", "TEXT"),
        # Self-healing retry state for the extraction pipeline.
        ("documents", "failure_class", "TEXT"),
        ("documents", "retry_count", "INTEGER NOT NULL DEFAULT 0"),
        ("documents", "next_retry_at", "TEXT"),
        # 0009: remaining-life revaluation fix.
        ("projects", "production_start_date", "TEXT"),
        ("revaluations", "remaining_life_years", "REAL"),
        # 0010: study NPV/tax extraction guard.
        ("studies", "needs_review", "INTEGER NOT NULL DEFAULT 0"),
        ("studies", "review_reason", "TEXT"),
        # 0011: parse-time extraction warnings (JSON array).
        ("studies", "extraction_warnings", "TEXT"),
    ]
    for table, col, col_type in additions:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
        except Exception:
            pass  # column already exists


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
