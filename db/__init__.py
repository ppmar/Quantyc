import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "quantyc.db"
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
    ]
    for table, col, col_type in additions:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
        except Exception:
            pass  # column already exists


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
