"""PR3: snapshot study-selection + reval-display SQL behavior.

Tests the exact query logic changed in api/snapshot.py (future-safe study
selection; reval bound to displayed study_id and gated to revaluable tiers)
against a minimal in-memory schema, without standing up the full 9-table route.
"""
import sqlite3
import pytest


# The two queries under test, copied verbatim from api/snapshot.py.
STUDY_SELECT = """
    SELECT study_id, study_stage, study_date
    FROM studies WHERE project_id = ?
    ORDER BY CASE WHEN study_date IS NULL OR study_date <= date('now') THEN 0 ELSE 1 END,
             study_date DESC LIMIT 1
"""
REVAL_SELECT = """
    SELECT r.revaluation_id, r.study_confidence_tier, r.npv_dfs
    FROM revaluations r
    JOIN studies s ON s.study_id = r.study_id
    WHERE r.study_id = ?
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
            study_id INTEGER, study_confidence_tier TEXT, npv_dfs REAL,
            computed_at TEXT
        );
    """)
    return c


def test_future_dated_study_not_selected(db):
    # DFS in the past, Scoping a year in the future.
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


def test_reval_excluded_for_conceptual_study(db):
    db.execute("INSERT INTO studies (study_id,project_id,study_stage,study_confidence_tier,study_date,post_tax_npv) VALUES (10,1,'Scoping','conceptual','2025-06-30',1178)")
    db.execute("INSERT INTO revaluations (study_id,study_confidence_tier,npv_dfs,computed_at) VALUES (10,'conceptual',1178,'2026-01-01')")
    db.commit()
    assert db.execute(REVAL_SELECT, (10,)).fetchone() is None


def test_reval_shown_for_definitive_study(db):
    db.execute("INSERT INTO studies (study_id,project_id,study_stage,study_confidence_tier,study_date,post_tax_npv) VALUES (20,1,'DFS','definitive','2024-06-15',451)")
    db.execute("INSERT INTO revaluations (study_id,study_confidence_tier,npv_dfs,computed_at) VALUES (20,'definitive',451,'2026-01-01')")
    db.commit()
    row = db.execute(REVAL_SELECT, (20,)).fetchone()
    assert row is not None
    assert row["study_confidence_tier"] == "definitive"
    assert row["npv_dfs"] == 451


def test_reval_bound_to_study_id(db):
    # A definitive reval on a DIFFERENT study must not show for this study.
    db.execute("INSERT INTO studies (study_id,project_id,study_stage,study_confidence_tier,study_date,post_tax_npv) VALUES (30,1,'DFS','definitive','2024-06-15',451)")
    db.execute("INSERT INTO revaluations (study_id,study_confidence_tier,npv_dfs,computed_at) VALUES (99,'definitive',999,'2026-01-01')")
    db.commit()
    assert db.execute(REVAL_SELECT, (30,)).fetchone() is None
