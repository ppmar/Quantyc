"""Tests for api/portfolio.py endpoints."""
import sqlite3
from unittest.mock import patch

import pytest

from tests._portfolio_db_setup import setup_test_db


@pytest.fixture
def client(tmp_path):
    """Flask test client with seeded test DB."""
    db_path = tmp_path / "test.db"
    setup_test_db(str(db_path)).close()

    def _get_test_connection():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    with patch("api.portfolio.get_connection", _get_test_connection):
        from api.portfolio import bp
        from flask import Flask
        app = Flask(__name__)
        app.register_blueprint(bp)
        app.config["TESTING"] = True

        with app.test_client() as c:
            yield c


class TestPortfolioCompanies:
    def test_returns_companies(self, client):
        resp = client.get("/api/portfolio/companies")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "companies" in data
        assert "total_companies" in data
        assert data["total_companies"] >= 1

    def test_response_structure(self, client):
        resp = client.get("/api/portfolio/companies")
        data = resp.get_json()
        if data["companies"]:
            c = data["companies"][0]
            assert "ticker" in c
            assert "active_project_count" in c
            assert "most_advanced_stage" in c
            assert "is_single_project" in c

    def test_single_project_filter(self, client):
        resp = client.get("/api/portfolio/companies?single_project_only=true")
        data = resp.get_json()
        for c in data["companies"]:
            assert c["is_single_project"] is True

    def test_recent_study_filter(self, client):
        resp = client.get("/api/portfolio/companies?has_recent_study=true")
        data = resp.get_json()
        for c in data["companies"]:
            assert c["has_recent_study"] is True

    def test_study_after_includes_on_or_after(self, client):
        # DEG's study is dated 2024-08-15; cutoff before it -> included.
        resp = client.get("/api/portfolio/companies?study_after=2024-01-01")
        data = resp.get_json()
        assert data["filters_applied"]["study_after"] == "2024-01-01"
        assert "DEG" in [c["ticker"] for c in data["companies"]]

    def test_study_after_excludes_before_cutoff(self, client):
        # Cutoff after DEG's 2024-08-15 study -> excluded.
        resp = client.get("/api/portfolio/companies?study_after=2025-01-01")
        data = resp.get_json()
        assert "DEG" not in [c["ticker"] for c in data["companies"]]

    def test_supported_only_keeps_au_dfs_drops_lithium(self, client):
        resp = client.get("/api/portfolio/companies?supported_only=true")
        data = resp.get_json()
        assert data["filters_applied"]["supported_only"] is True
        tickers = [c["ticker"] for c in data["companies"]]
        assert "DEG" in tickers      # Au + DFS (definitive)
        assert "LIT" not in tickers  # Li2O excluded despite having a DFS

    def test_supported_only_off_shows_lithium(self, client):
        resp = client.get("/api/portfolio/companies")
        data = resp.get_json()
        assert "LIT" in [c["ticker"] for c in data["companies"]]

    def test_has_dfs_pfs_flag(self, client):
        resp = client.get("/api/portfolio/companies")
        deg = next(c for c in resp.get_json()["companies"] if c["ticker"] == "DEG")
        assert deg["has_dfs_pfs"] is True

    def test_study_project_count_counts_only_projects_with_a_study(self, client):
        resp = client.get("/api/portfolio/companies")
        companies = {c["ticker"]: c for c in resp.get_json()["companies"]}
        # DEG/Hemi has a study → 1
        assert companies["DEG"]["study_project_count"] == 1
        # PRD/BigMine is resource-only (active, listed) but has no study → 0
        assert companies["PRD"]["study_project_count"] == 0
        # resource-only company is still listed (active_project_count keeps it)
        assert companies["PRD"]["active_project_count"] >= 1

    def test_excludes_companies_with_no_active_projects(self, client):
        resp = client.get("/api/portfolio/companies")
        data = resp.get_json()
        tickers = [c["ticker"] for c in data["companies"]]
        assert "ZZZ" not in tickers

    def test_most_advanced_stage_derived(self, client):
        resp = client.get("/api/portfolio/companies")
        data = resp.get_json()
        deg = next((c for c in data["companies"] if c["ticker"] == "DEG"), None)
        assert deg is not None
        assert deg["most_advanced_stage"] == "feasibility"

    def test_stage_filter_is_exact(self, client):
        # Selecting "feasibility" returns only feasibility-stage companies —
        # not more-advanced (production) nor less-advanced (exploration).
        resp = client.get("/api/portfolio/companies?min_stage=feasibility")
        tickers = [c["ticker"] for c in resp.get_json()["companies"]]
        assert "DEG" in tickers       # feasibility
        assert "PRD" not in tickers   # production — excluded
        assert "EXP" not in tickers   # exploration — excluded

    def test_stage_filter_exploration_only(self, client):
        resp = client.get("/api/portfolio/companies?min_stage=exploration")
        tickers = [c["ticker"] for c in resp.get_json()["companies"]]
        assert "EXP" in tickers
        assert "PRD" not in tickers
        assert "DEG" not in tickers


class TestPortfolioCompanyDetail:
    def test_returns_projects(self, client):
        resp = client.get("/api/portfolio/companies/DEG")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ticker"] == "DEG"
        assert "projects" in data
        assert len(data["projects"]) >= 1

    def test_project_structure(self, client):
        resp = client.get("/api/portfolio/companies/DEG")
        data = resp.get_json()
        proj = data["projects"][0]
        assert "project_name" in proj
        assert "stage" in proj
        assert "stage_source" in proj
        assert "is_active" in proj
        assert "document_counts" in proj

    def test_404_unknown_ticker(self, client):
        resp = client.get("/api/portfolio/companies/NOPE")
        assert resp.status_code == 404

    def test_stage_confidence_from_inference(self, client):
        resp = client.get("/api/portfolio/companies/DEG")
        data = resp.get_json()
        hemi = next((p for p in data["projects"] if p["project_name"] == "Hemi"), None)
        assert hemi is not None
        assert hemi["stage_confidence"] == "high"
        assert hemi["stage_source"] == "gemini_inferred"
