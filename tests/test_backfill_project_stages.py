"""Tests for scripts/backfill_project_stages.py"""
import json
import sqlite3
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest

from parsers.project_stage_classifier import (
    InsufficientEvidenceError,
    ProjectStageInference,
    ProjectEvidence,
)

# Import after sys.path is set up by conftest or pytest
import scripts.backfill_project_stages as backfill


# ─── Fixtures ────────────────────────────────────────────────────────

class _NoCloseConnection:
    """Wraps a sqlite3 connection to suppress close() during tests."""
    def __init__(self, conn):
        self._conn = conn

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def close(self):
        pass  # suppress

    def real_close(self):
        self._conn.close()


@pytest.fixture
def db(tmp_path):
    """In-memory SQLite with seed data."""
    db_path = tmp_path / "test.db"
    real_conn = sqlite3.connect(str(db_path))
    real_conn.row_factory = sqlite3.Row
    real_conn.execute("PRAGMA foreign_keys=ON")
    conn = _NoCloseConnection(real_conn)

    conn.executescript("""
        CREATE TABLE companies (
            company_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL UNIQUE,
            name TEXT,
            reporting_currency TEXT DEFAULT 'AUD',
            first_seen_at TEXT NOT NULL,
            last_updated_at TEXT NOT NULL
        );
        CREATE TABLE documents (
            document_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            url TEXT NOT NULL,
            sha256 TEXT NOT NULL UNIQUE,
            source TEXT NOT NULL,
            announcement_date TEXT,
            ingested_at TEXT NOT NULL,
            doc_type TEXT,
            header TEXT,
            parse_status TEXT NOT NULL DEFAULT 'pending',
            parse_error TEXT,
            local_path TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE projects (
            project_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL REFERENCES companies(company_id),
            project_name TEXT NOT NULL,
            country TEXT,
            state TEXT,
            stage TEXT,
            ownership_pct REAL,
            created_at TEXT NOT NULL,
            source TEXT,
            region TEXT,
            stage_source TEXT,
            stage_inferred_at TEXT
        );
        CREATE TABLE project_commodities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(project_id),
            commodity TEXT NOT NULL,
            is_primary INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE studies (
            study_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(project_id),
            document_id INTEGER REFERENCES documents(document_id),
            study_stage TEXT,
            study_date TEXT,
            study_confidence_tier TEXT,
            created_at TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE resources (
            resource_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(project_id),
            document_id INTEGER REFERENCES documents(document_id),
            effective_date TEXT NOT NULL,
            commodity TEXT NOT NULL,
            resource_or_reserve TEXT NOT NULL,
            category TEXT NOT NULL,
            tonnes REAL,
            grade REAL,
            grade_unit TEXT,
            contained_metal REAL,
            contained_metal_unit TEXT,
            created_at TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE project_stage_inferences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(project_id),
            stage TEXT,
            stage_confidence TEXT,
            region TEXT,
            reasoning TEXT,
            evidence_json TEXT NOT NULL,
            inferred_at TEXT NOT NULL
        );
    """)

    now = datetime.utcnow().isoformat()

    # Seed company
    conn.execute(
        "INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES (?, ?, ?, ?)",
        ("DEG", "De Grey Mining", now, now),
    )
    cid = conn.execute("SELECT company_id FROM companies WHERE ticker='DEG'").fetchone()[0]

    # Seed 3 projects
    for pname in ["Hemi", "Mallina", "Wingina"]:
        conn.execute(
            "INSERT INTO projects (company_id, project_name, country, state, created_at) VALUES (?, ?, ?, ?, ?)",
            (cid, pname, "Australia", "Western Australia", now),
        )

    pids = [r[0] for r in conn.execute("SELECT project_id FROM projects ORDER BY project_id").fetchall()]

    # Seed a study for Hemi
    conn.execute(
        "INSERT INTO documents (ticker, url, sha256, source, announcement_date, ingested_at, header, parse_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("DEG", "http://example.com/dfs.pdf", "sha-hemi-dfs", "asx_api", "2024-08-15", now, "Hemi DFS Results", "parsed"),
    )
    doc_id = conn.execute("SELECT document_id FROM documents LIMIT 1").fetchone()[0]
    conn.execute(
        "INSERT INTO studies (project_id, document_id, study_stage, study_date, study_confidence_tier, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (pids[0], doc_id, "DFS", "2024-08-15", "definitive", now),
    )

    # Seed a resource for Mallina
    conn.execute(
        "INSERT INTO resources (project_id, effective_date, commodity, resource_or_reserve, category, tonnes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (pids[1], "2024-01-01", "Au", "Resource", "Indicated", 50_000_000, now),
    )

    real_conn.commit()
    yield conn, db_path, pids
    conn.real_close()


def _mock_inference(stage="feasibility", confidence="high", region="Pilbara"):
    return ProjectStageInference(
        stage=stage,
        stage_confidence=confidence,
        region=region,
        reasoning=f"Test reasoning for {stage}.",
    )


# ─── Tests ───────────────────────────────────────────────────────────

class TestBuildEvidence:
    def test_builds_evidence_with_study(self, db):
        conn, db_path, pids = db
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            project = dict(conn.execute(
                "SELECT p.*, c.ticker FROM projects p JOIN companies c ON c.company_id = p.company_id WHERE p.project_id = ?",
                (pids[0],),
            ).fetchone())
            ev = backfill.build_evidence(project)
        assert len(ev.studies) == 1
        assert ev.studies[0].study_stage == "DFS"

    def test_builds_evidence_with_resource(self, db):
        conn, db_path, pids = db
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            project = dict(conn.execute(
                "SELECT p.*, c.ticker FROM projects p JOIN companies c ON c.company_id = p.company_id WHERE p.project_id = ?",
                (pids[1],),
            ).fetchone())
            ev = backfill.build_evidence(project)
        assert len(ev.resources) == 1
        assert ev.resources[0].commodity == "Au"

    def test_empty_evidence_for_project_with_nothing(self, db):
        conn, db_path, pids = db
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            project = dict(conn.execute(
                "SELECT p.*, c.ticker FROM projects p JOIN companies c ON c.company_id = p.company_id WHERE p.project_id = ?",
                (pids[2],),
            ).fetchone())
            ev = backfill.build_evidence(project)
        assert ev.is_empty()


class TestClassifyOne:
    def test_dry_run_no_api_call(self, db):
        conn, db_path, pids = db
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            project = dict(conn.execute(
                "SELECT p.*, c.ticker FROM projects p JOIN companies c ON c.company_id = p.company_id WHERE p.project_id = ?",
                (pids[0],),
            ).fetchone())
            result = backfill._classify_one(project, dry_run=True)
        assert result["status"] == "dry_run"

    def test_insufficient_evidence_persisted(self, db):
        conn, db_path, pids = db
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            project = dict(conn.execute(
                "SELECT p.*, c.ticker FROM projects p JOIN companies c ON c.company_id = p.company_id WHERE p.project_id = ?",
                (pids[2],),  # Wingina — no evidence
            ).fetchone())
            result = backfill._classify_one(project, dry_run=False)

        assert result["status"] == "insufficient"
        row = conn.execute(
            "SELECT stage_source, stage FROM projects WHERE project_id = ?", (pids[2],)
        ).fetchone()
        assert row["stage_source"] == "insufficient_evidence"
        assert row["stage"] is None  # unchanged

    @patch("scripts.backfill_project_stages.classify_project")
    def test_successful_classification_persisted(self, mock_classify, db):
        conn, db_path, pids = db
        inference = _mock_inference()
        mock_classify.return_value = inference

        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            project = dict(conn.execute(
                "SELECT p.*, c.ticker FROM projects p JOIN companies c ON c.company_id = p.company_id WHERE p.project_id = ?",
                (pids[0],),
            ).fetchone())
            result = backfill._classify_one(project, dry_run=False)

        assert result["status"] == "classified"
        assert result["stage"] == "feasibility"

        # Check DB updates
        row = conn.execute(
            "SELECT stage, stage_source, region FROM projects WHERE project_id = ?", (pids[0],)
        ).fetchone()
        assert row["stage"] == "feasibility"
        assert row["stage_source"] == "gemini_inferred"
        assert row["region"] == "Pilbara"

        # Check inference audit row
        inf_row = conn.execute(
            "SELECT * FROM project_stage_inferences WHERE project_id = ?", (pids[0],)
        ).fetchone()
        assert inf_row is not None
        assert inf_row["stage"] == "feasibility"
        assert inf_row["reasoning"] == "Test reasoning for feasibility."


class TestRunBackfill:
    @patch("scripts.backfill_project_stages.classify_project")
    def test_classifies_all_unclassified(self, mock_classify, db):
        conn, db_path, pids = db
        mock_classify.return_value = _mock_inference()
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            stats = backfill.run_backfill(workers=1)
        assert stats["classified"] == 3
        assert stats["stage_counts"].get("feasibility") == 3

    @patch("scripts.backfill_project_stages.classify_project")
    def test_idempotent_second_run_classifies_nothing(self, mock_classify, db):
        conn, db_path, pids = db
        mock_classify.return_value = _mock_inference()
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            backfill.run_backfill(workers=1)
            stats2 = backfill.run_backfill(workers=1)
        assert stats2["classified"] == 0

    def test_fetch_excludes_insufficient_evidence(self, db):
        conn, db_path, pids = db
        conn.execute(
            "UPDATE projects SET stage_source='insufficient_evidence' WHERE project_id=?",
            (pids[2],),
        )
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            default = backfill._fetch_projects(None, False, None)
            allp = backfill._fetch_projects(None, True, None)
        assert pids[2] not in [p["project_id"] for p in default]
        assert pids[2] in [p["project_id"] for p in allp]

    def test_fetch_includes_study_floor(self, db):
        """A study_floor project must be re-eligible so a later Gemini pass can
        lift it past feasibility (e.g. to production). Floored != final."""
        conn, db_path, pids = db
        conn.execute(
            "UPDATE projects SET stage='feasibility', stage_source='study_floor' WHERE project_id=?",
            (pids[0],),
        )
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            default = backfill._fetch_projects(None, False, None)
        assert pids[0] in [p["project_id"] for p in default]

    def test_skip_cached_study_floor_no_new_evidence(self, db):
        """A study_floor project already attempted (stage_inferred_at set) with no
        new evidence is cached — no repeat Gemini call each run."""
        conn, db_path, pids = db
        conn.execute(
            "UPDATE projects SET stage='feasibility', stage_source='study_floor', "
            "stage_inferred_at='2099-01-01T00:00:00' WHERE project_id=?",
            (pids[0],),
        )
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            project = dict(conn.execute(
                "SELECT p.*, c.ticker FROM projects p JOIN companies c ON c.company_id=p.company_id "
                "WHERE p.project_id=?", (pids[0],),
            ).fetchone())
            assert backfill._should_skip_cached(project) is True


class TestStudyFloor:
    """PR1: a definitive/indicative study floors the project to >= feasibility,
    regardless of what the LLM returns. Floor never downgrades, audit keeps raw LLM."""

    @patch("scripts.backfill_project_stages.classify_project")
    def test_floor_lifts_unknown_to_feasibility(self, mock_classify, db):
        # RMX/Batangas case: DFS present, classifier returns unknown.
        conn, db_path, pids = db
        mock_classify.return_value = _mock_inference(stage="unknown", region=None)
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            project = dict(conn.execute(
                "SELECT p.*, c.ticker FROM projects p JOIN companies c ON c.company_id = p.company_id WHERE p.project_id = ?",
                (pids[0],),  # Hemi — has definitive study
            ).fetchone())
            backfill._classify_one(project, dry_run=False)

        row = conn.execute(
            "SELECT stage, stage_source FROM projects WHERE project_id = ?", (pids[0],)
        ).fetchone()
        assert row["stage"] == "feasibility"
        assert row["stage_source"] == "study_floor"

        # Audit row preserves the RAW LLM stage.
        inf = conn.execute(
            "SELECT stage FROM project_stage_inferences WHERE project_id = ? ORDER BY id DESC LIMIT 1",
            (pids[0],),
        ).fetchone()
        assert inf["stage"] == "unknown"

    @patch("scripts.backfill_project_stages.classify_project")
    def test_floor_never_downgrades_production(self, mock_classify, db):
        conn, db_path, pids = db
        mock_classify.return_value = _mock_inference(stage="production", region=None)
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            project = dict(conn.execute(
                "SELECT p.*, c.ticker FROM projects p JOIN companies c ON c.company_id = p.company_id WHERE p.project_id = ?",
                (pids[0],),
            ).fetchone())
            backfill._classify_one(project, dry_run=False)

        row = conn.execute(
            "SELECT stage, stage_source FROM projects WHERE project_id = ?", (pids[0],)
        ).fetchone()
        assert row["stage"] == "production"
        assert row["stage_source"] == "gemini_inferred"  # floor did not win

    @patch("scripts.backfill_project_stages.classify_project")
    def test_insufficient_evidence_still_floors_when_study_exists(self, mock_classify, db):
        # Classifier gives nothing, but the project has a definitive study.
        conn, db_path, pids = db
        mock_classify.side_effect = InsufficientEvidenceError("no signal")
        with patch("scripts.backfill_project_stages.get_connection", return_value=conn):
            project = dict(conn.execute(
                "SELECT p.*, c.ticker FROM projects p JOIN companies c ON c.company_id = p.company_id WHERE p.project_id = ?",
                (pids[0],),  # Hemi — has definitive study
            ).fetchone())
            backfill._classify_one(project, dry_run=False)

        row = conn.execute(
            "SELECT stage, stage_source FROM projects WHERE project_id = ?", (pids[0],)
        ).fetchone()
        assert row["stage"] == "feasibility"
        assert row["stage_source"] == "study_floor"
