"""PR3: snapshot study-selection + reval-display SQL behavior.

Tests the exact query logic in api/snapshot.py (future-safe study selection; reval =
latest revaluable study of the PROJECT, decoupled from the displayed study) against a
minimal in-memory schema. The real-route counterpart is test_snapshot_route_reval.py.
"""
import sqlite3
import pytest


# Copied verbatim from api/snapshot.py (study selection unchanged).
STUDY_SELECT = """
    SELECT study_id, study_stage, study_date
    FROM studies WHERE project_id = ?
    ORDER BY CASE WHEN study_date IS NULL OR study_date <= date('now') THEN 0 ELSE 1 END,
             study_date DESC LIMIT 1
"""
# Project-scoped reval (decoupled from displayed study). Reduced projection; no
# commodity_prices / reporting_currency joins for the in-memory schema.
REVAL_SELECT = """
    WITH latest_revaluable_study AS (
        SELECT study_id
        FROM studies
        WHERE project_id = ?
          AND study_confidence_tier IN ('definitive', 'indicative')
        ORDER BY CASE WHEN study_date IS NULL OR study_date <= date('now')
                      THEN 0 ELSE 1 END,
                 study_date DESC
        LIMIT 1
    )
    SELECT r.revaluation_id, r.study_confidence_tier, r.npv_dfs
    FROM revaluations r
    WHERE r.study_id = (SELECT study_id FROM latest_revaluable_study)
      AND r.study_confidence_tier IN ('definitive', 'indicative')
    ORDER BY r.computed_at DESC LIMIT 1
"""


@pytest.fixture
def db():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE studies (
            study_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER, study_stage TEXT, study_confidence_tier TEXT,
            study_date TEXT, post_tax_npv REAL
        );
        CREATE TABLE revaluations (
            revaluation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id INTEGER, project_id INTEGER,
            study_confidence_tier TEXT, npv_dfs REAL,
            computed_at TEXT
        );
    """)
    return c


# ── study selection (unchanged behavior) ──────────────────────────

def test_future_dated_study_not_selected(db):
    db.execute("INSERT INTO studies (project_id,study_stage,study_confidence_tier,study_date,post_tax_npv) VALUES (1,'DFS','definitive','2024-06-15',451)")
    db.execute("INSERT INTO studies (project_id,study_stage,study_confidence_tier,study_date,post_tax_npv) VALUES (1,'Scoping','conceptual',date('now','+365 day'),1178)")
    db.commit()
    row = db.execute(STUDY_SELECT, (1,)).fetchone()
    assert row["study_stage"] == "DFS"
    assert row["study_date"] == "2024-06-15"


def test_null_dated_study_selectable(db):
    db.execute("INSERT INTO studies (project_id,study_stage,study_confidence_tier,study_date,post_tax_npv) VALUES (1,'DFS','definitive',NULL,451)")
    db.commit()
    assert db.execute(STUDY_SELECT, (1,)).fetchone()["study_stage"] == "DFS"


# ── reval decoupled from displayed study (D2) ─────────────────────

def test_reval_decoupled_shows_dfs_under_later_scoping(db):
    # Revalued DFS, then a LATER Scoping on the same project.
    db.execute("INSERT INTO studies (study_id,project_id,study_stage,study_confidence_tier,study_date,post_tax_npv) VALUES (1,7,'DFS','definitive','2024-06-15',451)")
    db.execute("INSERT INTO studies (study_id,project_id,study_stage,study_confidence_tier,study_date,post_tax_npv) VALUES (2,7,'Scoping','conceptual','2025-03-01',1178)")
    db.execute("INSERT INTO revaluations (study_id,project_id,study_confidence_tier,npv_dfs,computed_at) VALUES (1,7,'definitive',451,'2024-07-01')")
    db.commit()
    row = db.execute(REVAL_SELECT, (7,)).fetchone()
    assert row is not None
    assert row["study_confidence_tier"] == "definitive"
    assert row["npv_dfs"] == 451


def test_reval_none_when_no_revaluable_study(db):
    # Scoping only, plus a stray conceptual reval row -> still None (I7).
    db.execute("INSERT INTO studies (study_id,project_id,study_stage,study_confidence_tier,study_date,post_tax_npv) VALUES (3,9,'Scoping','conceptual','2025-01-01',100)")
    db.execute("INSERT INTO revaluations (study_id,project_id,study_confidence_tier,npv_dfs,computed_at) VALUES (3,9,'conceptual',100,'2025-02-01')")
    db.commit()
    assert db.execute(REVAL_SELECT, (9,)).fetchone() is None


def test_reval_latest_revaluable_when_two_dfs(db):
    # Two DFS, the newer one revalued -> the newer reval wins.
    db.execute("INSERT INTO studies (study_id,project_id,study_stage,study_confidence_tier,study_date,post_tax_npv) VALUES (10,5,'DFS','definitive','2023-01-01',300)")
    db.execute("INSERT INTO studies (study_id,project_id,study_stage,study_confidence_tier,study_date,post_tax_npv) VALUES (11,5,'Updated DFS','definitive','2025-01-01',600)")
    db.execute("INSERT INTO revaluations (study_id,project_id,study_confidence_tier,npv_dfs,computed_at) VALUES (10,5,'definitive',300,'2023-02-01')")
    db.execute("INSERT INTO revaluations (study_id,project_id,study_confidence_tier,npv_dfs,computed_at) VALUES (11,5,'definitive',600,'2025-02-01')")
    db.commit()
    assert db.execute(REVAL_SELECT, (5,)).fetchone()["npv_dfs"] == 600
