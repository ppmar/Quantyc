"""Deterministic production-floor sweep over the DB (no LLM)."""
import sqlite3

import pytest

from scripts.backfill_project_stages import apply_production_floors


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(
        """
        CREATE TABLE companies (company_id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT UNIQUE);
        CREATE TABLE projects (project_id INTEGER PRIMARY KEY AUTOINCREMENT, company_id INTEGER,
            project_name TEXT, stage TEXT, stage_source TEXT, stage_inferred_at TEXT,
            production_start_date TEXT);
        CREATE TABLE studies (study_id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER,
            study_confidence_tier TEXT, study_date TEXT);
        CREATE TABLE company_financials (financial_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER, effective_date TEXT, receipts_from_customers REAL);
        """
    )
    return c


def _co(c, t):
    return c.execute("INSERT INTO companies (ticker) VALUES (?)", (t,)).lastrowid

def _pj(c, cid, name, stage="feasibility", date_=None):
    return c.execute(
        "INSERT INTO projects (company_id, project_name, stage, stage_source, production_start_date) "
        "VALUES (?, ?, ?, 'study_floor', ?)", (cid, name, stage, date_)
    ).lastrowid

def _study(c, pid, tier="definitive"):
    c.execute("INSERT INTO studies (project_id, study_confidence_tier, study_date) VALUES (?, ?, '2024-01-01')", (pid, tier))
    c.commit()

def _fin(c, cid, receipts, d="2026-03-31"):
    c.execute("INSERT INTO company_financials (company_id, effective_date, receipts_from_customers) VALUES (?, ?, ?)", (cid, d, receipts))
    c.commit()

def _stage(c, pid):
    return c.execute("SELECT stage, stage_source FROM projects WHERE project_id=?", (pid,)).fetchone()


def test_material_receipts_promotes_single_project(conn):
    cid = _co(conn, "MEK"); pid = _pj(conn, cid, "Murchison"); _study(conn, pid)
    _fin(conn, cid, 40_000_000)  # selling gold
    apply_production_floors(conn)
    r = _stage(conn, pid)
    assert r["stage"] == "production"
    assert r["stage_source"] == "production_floor"


def test_material_receipts_promotes_without_study(conn):
    # A$40M customer receipts => producer, even with no DFS on file (incomplete
    # study coverage, e.g. GMD/BGL/LYC). Receipts self-prove.
    cid = _co(conn, "PROD"); pid = _pj(conn, cid, "Mine", stage="advanced_exploration")  # no study
    _fin(conn, cid, 40_000_000)
    apply_production_floors(conn)
    assert _stage(conn, pid)["stage"] == "production"


def test_implausible_receipts_not_promoted(conn):
    # VIT-style misparse (A$26B) must not flip a company to production.
    cid = _co(conn, "BAD"); pid = _pj(conn, cid, "Glitch"); _study(conn, pid)
    _fin(conn, cid, 26_000_000_000)
    apply_production_floors(conn)
    assert _stage(conn, pid)["stage"] == "feasibility"


def test_passed_production_date_promotes_that_project(conn):
    cid = _co(conn, "DEV"); pid = _pj(conn, cid, "BuiltMine", date_="2025-06-01"); _study(conn, pid)
    apply_production_floors(conn)  # no receipts at all
    assert _stage(conn, pid)["stage"] == "production"


def test_future_production_date_not_promoted(conn):
    cid = _co(conn, "SOON"); pid = _pj(conn, cid, "Later", date_="2027-01-01"); _study(conn, pid)
    apply_production_floors(conn)
    assert _stage(conn, pid)["stage"] == "feasibility"


def test_receipts_attributed_to_built_project_only(conn):
    """Company-level receipts promote only the project with a revaluable study,
    not a sibling with no study."""
    cid = _co(conn, "MULTI")
    built = _pj(conn, cid, "BuiltMine"); _study(conn, built)
    other = _pj(conn, cid, "EarlyGround", stage="advanced_exploration")  # no study
    _fin(conn, cid, 40_000_000)
    apply_production_floors(conn)
    assert _stage(conn, built)["stage"] == "production"
    assert _stage(conn, other)["stage"] == "advanced_exploration"


def test_idempotent(conn):
    cid = _co(conn, "MEK"); pid = _pj(conn, cid, "Murchison"); _study(conn, pid)
    _fin(conn, cid, 40_000_000)
    s1 = apply_production_floors(conn)
    s2 = apply_production_floors(conn)
    assert s1["promoted"] == 1
    assert s2["promoted"] == 0  # already production
    assert _stage(conn, pid)["stage"] == "production"
