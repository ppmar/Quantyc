import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "staging.db"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _migrate(conn: sqlite3.Connection):
    """Run lightweight schema migrations for existing databases."""
    # Add original_filename column if missing (dedup support)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()]
    if "original_filename" not in cols:
        conn.execute("ALTER TABLE documents ADD COLUMN original_filename TEXT")
    # Unique index on (ticker, filename) for dedup
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_ticker_filename "
        "ON documents(company_ticker, original_filename) WHERE original_filename IS NOT NULL"
    )
    conn.commit()


def init_db():
    conn = get_connection()
    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())
    _migrate(conn)
    conn.close()


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
