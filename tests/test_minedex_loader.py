"""Tests for MINEDEX bootstrap loader — synthetic fixtures, no network calls."""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingest.minedex_loader import load_minedex

FIXTURE_CSV = Path(__file__).parent / "fixtures" / "minedex" / "sample_extract.csv"


class _NoCloseConn:
    """Proxy that forwards everything to a real connection but ignores close()."""
    def __init__(self, conn):
        self._conn = conn
    def close(self):
        pass
    def __getattr__(self, name):
        return getattr(self._conn, name)


@pytest.fixture
def in_memory_db(monkeypatch):
    """Set up an in-memory SQLite DB with schema and seed companies."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        CREATE TABLE companies (
            company_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL UNIQUE,
            name TEXT,
            reporting_currency TEXT DEFAULT 'AUD',
            fiscal_year_end TEXT,
            first_seen_at TEXT NOT NULL,
            last_updated_at TEXT NOT NULL
        );
        CREATE TABLE projects (
            project_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL REFERENCES companies(company_id),
            project_name TEXT NOT NULL,
            country TEXT,
            state TEXT,
            stage TEXT,
            ownership_pct REAL,
            source TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE project_commodities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(project_id),
            commodity TEXT NOT NULL,
            is_primary INTEGER NOT NULL DEFAULT 0
        );
    """)

    now = datetime.now(timezone.utc).isoformat()
    for ticker, name in [("DEG", "De Grey Mining"), ("LTR", "Liontown Resources"),
                          ("PLS", "Pilbara Minerals"), ("LYC", "Lynas Rare Earths")]:
        conn.execute(
            "INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES (?, ?, ?, ?)",
            (ticker, name, now, now),
        )
    conn.commit()

    proxy = _NoCloseConn(conn)
    import ingest.minedex_loader as loader_mod
    monkeypatch.setattr(loader_mod, "get_connection", lambda: proxy)
    import ingest.ozmin_loader as ozmin_mod
    monkeypatch.setattr(ozmin_mod, "get_connection", lambda: proxy)

    yield conn
    conn.close()


@pytest.fixture
def sample_rows():
    import csv
    with open(FIXTURE_CSV, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


class TestLoadMinedex:
    def test_inserts_known_projects(self, in_memory_db, sample_rows):
        stats = load_minedex(rows=sample_rows)

        projects = in_memory_db.execute("SELECT * FROM projects").fetchall()
        assert len(projects) >= 3  # Hemi, Kathleen Valley, Pilgangoora, Mount Weld

        kv = in_memory_db.execute(
            "SELECT * FROM projects WHERE LOWER(project_name) = 'kathleen valley'"
        ).fetchone()
        assert kv is not None
        assert kv["source"] == "minedex"
        assert kv["state"] == "WA"
        assert kv["stage"] == "development"

    def test_skips_unmapped_operators(self, in_memory_db, sample_rows):
        stats = load_minedex(rows=sample_rows)
        assert stats["skipped_unmapped"] >= 1

    def test_idempotent(self, in_memory_db, sample_rows):
        load_minedex(rows=sample_rows)
        count1 = in_memory_db.execute("SELECT COUNT(*) FROM projects").fetchone()[0]

        load_minedex(rows=sample_rows)
        count2 = in_memory_db.execute("SELECT COUNT(*) FROM projects").fetchone()[0]

        assert count1 == count2

    def test_preserves_ozmin_data(self, in_memory_db, sample_rows):
        # Pre-insert from OZMIN
        now = datetime.now(timezone.utc).isoformat()
        deg_id = in_memory_db.execute(
            "SELECT company_id FROM companies WHERE ticker = 'DEG'"
        ).fetchone()["company_id"]
        in_memory_db.execute(
            """INSERT INTO projects (company_id, project_name, country, state, stage, source, created_at)
               VALUES (?, 'Hemi Gold', 'Australia', 'WA', 'advanced_exploration', 'ozmin', ?)""",
            (deg_id, now),
        )
        in_memory_db.commit()

        load_minedex(rows=sample_rows)

        # OZMIN row should keep its stage
        hemi = in_memory_db.execute(
            "SELECT * FROM projects WHERE LOWER(project_name) = 'hemi gold'"
        ).fetchone()
        assert hemi["stage"] == "advanced_exploration"
        assert hemi["source"] == "ozmin"

    def test_commodities_inserted(self, in_memory_db, sample_rows):
        load_minedex(rows=sample_rows)

        pilg = in_memory_db.execute(
            "SELECT project_id FROM projects WHERE LOWER(project_name) = 'pilgangoora'"
        ).fetchone()
        assert pilg is not None

        comms = in_memory_db.execute(
            "SELECT commodity, is_primary FROM project_commodities WHERE project_id = ?",
            (pilg["project_id"],),
        ).fetchall()
        assert len(comms) >= 1
