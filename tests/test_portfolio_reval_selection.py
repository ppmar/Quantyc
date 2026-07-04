"""Company-list revaluation selection (PDI 599% vs 172% bug).

The companies screener must surface the revaluation of the company's latest
REVALUABLE study (most-recent definitive/indicative), matching the company
page — not whichever revaluation row happens to have the newest computed_at.
A superseded PFS (small base → huge %) must not win over the current DFS.
"""
import sqlite3
from unittest.mock import patch

import pytest


def _seed(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE companies (company_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT UNIQUE, name TEXT);
        CREATE TABLE documents (document_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT, announcement_date TEXT);
        CREATE TABLE projects (project_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER, project_name TEXT, country TEXT, state TEXT,
            region TEXT, stage TEXT);
        CREATE TABLE project_commodities (id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER, commodity TEXT, is_primary INTEGER DEFAULT 0);
        CREATE TABLE studies (study_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER, document_id INTEGER, study_stage TEXT,
            study_confidence_tier TEXT, study_date TEXT, post_tax_npv REAL,
            reporting_currency TEXT, needs_review INTEGER DEFAULT 0, review_reason TEXT);
        CREATE TABLE resources (resource_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER, commodity TEXT);
        CREATE TABLE revaluations (revaluation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id INTEGER, company_id INTEGER, computed_at TEXT, commodity TEXT,
            price_dfs REAL, price_spot REAL, npv_dfs REAL, npv_spot REAL, npv_uplift REAL,
            npv_uplift_pct REAL, warnings TEXT);
        """
    )
    cid = conn.execute("INSERT INTO companies (ticker, name) VALUES ('PDI','Predictive')").lastrowid
    pid = conn.execute(
        "INSERT INTO projects (company_id, project_name, stage) VALUES (?, 'Bankan', 'feasibility')",
        (cid,),
    ).lastrowid
    conn.execute("INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (?, 'Au', 1)", (pid,))
    # Older PFS (small base) and newer DFS (large base), both revaluable.
    pfs = conn.execute(
        "INSERT INTO studies (project_id, study_stage, study_confidence_tier, study_date, "
        "post_tax_npv, reporting_currency) VALUES (?, 'PFS', 'indicative', '2024-04-14', 668, 'AUD')",
        (pid,),
    ).lastrowid
    dfs = conn.execute(
        "INSERT INTO studies (project_id, study_stage, study_confidence_tier, study_date, "
        "post_tax_npv, reporting_currency) VALUES (?, 'DFS', 'definitive', '2025-06-24', 1637, 'AUD')",
        (pid,),
    ).lastrowid
    # DFS reval written first; PFS reval written microseconds LATER (the trap).
    conn.execute(
        "INSERT INTO revaluations (study_id, company_id, computed_at, commodity, price_spot, "
        "npv_dfs, npv_spot, npv_uplift, npv_uplift_pct) "
        "VALUES (?, ?, '2026-06-12T06:54:51.900000', 'Au', 4449.19, 1637, 4449.19, 2812.19, 1.7179)",
        (dfs, cid),
    )
    conn.execute(
        "INSERT INTO revaluations (study_id, company_id, computed_at, commodity, price_spot, "
        "npv_dfs, npv_spot, npv_uplift, npv_uplift_pct) "
        "VALUES (?, ?, '2026-06-12T06:54:51.910251', 'Au', 4670.83, 668, 4670.83, 4002.83, 5.9923)",
        (pfs, cid),
    )
    conn.commit()
    conn.close()


@pytest.fixture
def client(tmp_path):
    db_path = tmp_path / "reval.db"
    _seed(str(db_path))

    def _get_conn():
        c = sqlite3.connect(str(db_path))
        c.row_factory = sqlite3.Row
        return c

    with patch("api.portfolio.get_connection", _get_conn):
        from api.portfolio import bp
        from flask import Flask
        app = Flask(__name__)
        app.register_blueprint(bp)
        with app.test_client() as c:
            yield c


def test_list_uses_latest_revaluable_study_not_newest_write(client):
    data = client.get("/api/portfolio/companies").get_json()
    pdi = next(c for c in data["companies"] if c["ticker"] == "PDI")
    rv = pdi["latest_revaluation"]
    assert rv is not None
    # DFS (2025, base 1637) → 172%, NOT the superseded PFS (base 668) → 599%.
    assert rv["npv_uplift_pct"] == pytest.approx(1.7179)
    assert rv["npv_dfs"] == 1637
