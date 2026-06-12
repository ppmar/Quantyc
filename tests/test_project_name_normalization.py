"""Project-name normalization must apply to BOTH sides of the lookup.

R6 stripped commodity/qualifier suffixes from the incoming name only, so a
legacy stored row ("Paris Silver", "Rebecca-Roe Gold") never matched its clean
twin and the project forked — same study landing twice with different numbers
(IVR Paris, RMS Rebecca-Roe).
"""
from datetime import datetime, timezone

import pytest

from db import get_connection, init_db
from pipeline.orchestrator import normalize_project_name, _get_or_create_project


# ── Pure normalization ────────────────────────────────────────────────

def test_strips_commodity_suffix():
    assert normalize_project_name("Paris Silver") == "Paris"
    assert normalize_project_name("Rebecca-Roe Gold") == "Rebecca-Roe"


def test_strips_stacked_qualifiers():
    assert normalize_project_name("Syama Gold Project") == "Syama"


def test_keeps_scope_words():
    # Underground/Expansion/Stage are real sub-projects — never stripped.
    assert normalize_project_name("Kanowna Belle Underground") == "Kanowna Belle Underground"


def test_clean_name_unchanged():
    assert normalize_project_name("Hemi") == "Hemi"


# ── Lookup matches normalized stored names ────────────────────────────

@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("db.DB_PATH", db_path)
    init_db()
    yield


def _seed_company() -> int:
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO companies (ticker, first_seen_at, last_updated_at) VALUES ('IVR', ?, ?)",
        (now, now),
    )
    cid = cur.lastrowid
    conn.commit()
    conn.close()
    return cid


def test_legacy_suffixed_row_matches_clean_incoming():
    cid = _seed_company()
    conn = get_connection()
    conn.execute(
        "INSERT INTO projects (company_id, project_name, created_at) VALUES (?, 'Paris Silver', ?)",
        (cid, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    pid_existing = conn.execute("SELECT project_id FROM projects").fetchone()[0]

    pid = _get_or_create_project(conn, cid, "Paris")
    assert pid == pid_existing
    n = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    conn.close()
    assert n == 1


def test_suffixed_incoming_matches_clean_row():
    cid = _seed_company()
    conn = get_connection()
    conn.execute(
        "INSERT INTO projects (company_id, project_name, created_at) VALUES (?, 'Paris', ?)",
        (cid, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    pid_existing = conn.execute("SELECT project_id FROM projects").fetchone()[0]

    pid = _get_or_create_project(conn, cid, "Paris Silver Project")
    assert pid == pid_existing
    conn.close()
