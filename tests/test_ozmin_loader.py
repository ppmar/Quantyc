"""Tests for OZMIN bootstrap loader — synthetic fixtures, no network calls."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingest.ozmin_loader import normalize_operator, load_ozmin, _normalize_stage

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "ozmin" / "sample_response.json"


class _NoCloseConn:
    """Proxy that forwards everything to a real connection but ignores close()."""
    def __init__(self, conn):
        self._conn = conn
    def close(self):
        pass  # no-op
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
    conn.execute(
        "INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES (?, ?, ?, ?)",
        ("DEG", "De Grey Mining", now, now),
    )
    conn.execute(
        "INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES (?, ?, ?, ?)",
        ("PLS", "Pilbara Minerals", now, now),
    )
    # BOE intentionally NOT inserted — tests "mapped but no company row"
    conn.commit()

    proxy = _NoCloseConn(conn)
    import ingest.ozmin_loader as loader_mod
    monkeypatch.setattr(loader_mod, "get_connection", lambda: proxy)

    yield conn
    conn.close()


@pytest.fixture
def sample_features():
    with open(FIXTURE_PATH) as f:
        data = json.load(f)
    return data["features"]


class TestNormalize:
    def test_normalize_operator_strips_suffixes(self):
        assert normalize_operator("De Grey Mining Limited") == "de grey mining"
        assert normalize_operator("Pilbara Minerals Ltd") == "pilbara minerals"
        assert normalize_operator("  Boss Energy Ltd.  ") == "boss energy"
        assert normalize_operator("Lynas Rare Earths Pty") == "lynas rare earths"

    def test_normalize_collapses_whitespace(self):
        assert normalize_operator("De  Grey   Mining   Limited") == "de grey mining"


class TestStageNormalization:
    @pytest.mark.parametrize("status,expected", [
        ("Operating Mine", "production"),
        ("Producer", "production"),
        ("Production", "production"),
        ("Care and Maintenance", "care_and_maintenance"),
        ("Construction", "development"),
        ("Development", "development"),
        ("Feasibility", "feasibility"),
        ("PFS", "feasibility"),
        ("DFS", "feasibility"),
        ("Resource Definition", "advanced_exploration"),
        ("Advanced Exploration", "advanced_exploration"),
        ("Exploration", "exploration"),
        ("Prospect", "exploration"),
        ("Unknown Status", None),
        ("", None),
    ])
    def test_stage_mapping(self, status, expected):
        assert _normalize_stage(status) == expected


class TestLoadOzmin:
    def test_load_inserts_known_projects(self, in_memory_db, sample_features):
        stats = load_ozmin(features=sample_features)

        projects = in_memory_db.execute("SELECT * FROM projects").fetchall()
        assert len(projects) >= 2  # Hemi + Pilgangoora

        hemi = in_memory_db.execute(
            "SELECT * FROM projects WHERE LOWER(project_name) = 'hemi'"
        ).fetchone()
        assert hemi is not None
        assert hemi["source"] == "ozmin"
        assert hemi["state"] == "WA"
        assert hemi["country"] == "Australia"

    def test_load_skips_unmapped_operators(self, in_memory_db, sample_features):
        stats = load_ozmin(features=sample_features)
        assert stats["skipped_unmapped"] >= 1

        # "Unknown Exploration Pty Ltd" should not create a project
        unknown = in_memory_db.execute(
            "SELECT * FROM projects WHERE LOWER(project_name) = 'wombat creek'"
        ).fetchone()
        assert unknown is None

    def test_load_skips_unknown_tickers(self, in_memory_db, sample_features):
        # BOE is mapped in CSV but has no companies row
        stats = load_ozmin(features=sample_features)
        assert stats["skipped_no_company"] >= 1

    def test_load_is_idempotent(self, in_memory_db, sample_features):
        load_ozmin(features=sample_features)
        count1 = in_memory_db.execute("SELECT COUNT(*) FROM projects").fetchone()[0]

        load_ozmin(features=sample_features)
        count2 = in_memory_db.execute("SELECT COUNT(*) FROM projects").fetchone()[0]

        assert count1 == count2

    def test_load_preserves_existing_non_null_fields(self, in_memory_db, sample_features):
        # Pre-insert a project with stage set by parser
        now = datetime.now(timezone.utc).isoformat()
        deg_id = in_memory_db.execute(
            "SELECT company_id FROM companies WHERE ticker = 'DEG'"
        ).fetchone()["company_id"]

        in_memory_db.execute(
            """INSERT INTO projects (company_id, project_name, stage, source, created_at)
               VALUES (?, 'Hemi', 'exploration', 'jorc_parser', ?)""",
            (deg_id, now),
        )
        in_memory_db.commit()

        load_ozmin(features=sample_features)

        hemi = in_memory_db.execute(
            "SELECT * FROM projects WHERE LOWER(project_name) = 'hemi'"
        ).fetchone()
        # Stage should still be 'exploration' (parser data wins)
        assert hemi["stage"] == "exploration"

    def test_primary_commodity_marker(self, in_memory_db, sample_features):
        load_ozmin(features=sample_features)

        # Pilgangoora has "Lithium, Tantalum" — first should be primary
        pilg = in_memory_db.execute(
            "SELECT project_id FROM projects WHERE LOWER(project_name) = 'pilgangoora'"
        ).fetchone()
        assert pilg is not None

        commodities = in_memory_db.execute(
            "SELECT commodity, is_primary FROM project_commodities WHERE project_id = ? ORDER BY id",
            (pilg["project_id"],),
        ).fetchall()
        assert len(commodities) >= 2
        assert commodities[0]["is_primary"] == 1
        assert commodities[1]["is_primary"] == 0
