"""PR3: DFS-uplift signal must not let a tiny-base % top the screen."""
import sqlite3
from datetime import datetime
from unittest.mock import patch

import pytest

from tests._portfolio_db_setup import setup_test_db


def _seed_company(conn, ticker, npv_dfs, npv_spot, uplift_pct):
    now = datetime.utcnow().isoformat()
    conn.execute(
        "INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES (?, ?, ?, ?)",
        (ticker, f"{ticker} Co", now, now),
    )
    cid = conn.execute("SELECT company_id FROM companies WHERE ticker=?", (ticker,)).fetchone()[0]
    conn.execute(
        "INSERT INTO projects (company_id, project_name, country, stage, stage_source, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (cid, f"{ticker}-Project", "Australia", "feasibility", "gemini_inferred", now),
    )
    pid = conn.execute("SELECT project_id FROM projects WHERE company_id=?", (cid,)).fetchone()[0]
    conn.execute(
        "INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (?, ?, 1)",
        (pid, "Au"),
    )
    conn.execute(
        "INSERT INTO documents (ticker, url, sha256, source, announcement_date, ingested_at, header, parse_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (ticker, f"http://x/{ticker}.pdf", f"sha-{ticker}", "asx_api", "2024-08-15", now, f"{ticker} DFS", "parsed"),
    )
    doc_id = conn.execute("SELECT document_id FROM documents WHERE ticker=?", (ticker,)).fetchone()[0]
    conn.execute(
        "INSERT INTO studies (project_id, document_id, study_stage, study_confidence_tier, study_date, post_tax_npv, reporting_currency) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (pid, doc_id, "DFS", "definitive", "2024-08-15", npv_dfs, "AUD"),
    )
    sid = conn.execute("SELECT study_id FROM studies WHERE project_id=?", (pid,)).fetchone()[0]
    conn.execute(
        """INSERT INTO revaluations
           (study_id, project_id, company_id, computed_at, commodity, price_dfs, price_spot,
            npv_dfs, npv_spot, npv_uplift, npv_uplift_pct, method_version)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (sid, pid, cid, now, "Au", 1250.0, 4500.0,
         npv_dfs, npv_spot, npv_spot - npv_dfs, uplift_pct, "first_order_v3"),
    )


@pytest.fixture
def client(tmp_path):
    db_path = tmp_path / "test.db"
    conn = setup_test_db(str(db_path))
    # A: tiny base, huge %.   B: full base, modest %.
    _seed_company(conn, "A", npv_dfs=18.0, npv_spot=263.0, uplift_pct=13.90)
    _seed_company(conn, "B", npv_dfs=451.0, npv_spot=2412.0, uplift_pct=4.35)
    conn.commit()
    conn.close()

    def _get_test_connection():
        c = sqlite3.connect(str(db_path))
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys=ON")
        return c

    with patch("api.portfolio.get_connection", _get_test_connection):
        from api.portfolio import bp
        from flask import Flask
        app = Flask(__name__)
        app.register_blueprint(bp)
        app.config["TESTING"] = True
        with app.test_client() as c:
            yield c


def _order(client, sort):
    resp = client.get(f"/api/portfolio/companies?sort={sort}")
    assert resp.status_code == 200
    return [c["ticker"] for c in resp.get_json()["companies"]]


def _company(client, ticker):
    resp = client.get("/api/portfolio/companies")
    for c in resp.get_json()["companies"]:
        if c["ticker"] == ticker:
            return c
    raise AssertionError(f"{ticker} not in screen")


def test_pct_sort_is_pure_percent(client):
    """Pure % sort — no low-base sinking (Pmar decision, spec_screener_signal_visibility).
    Signal quality is carried visually (age dot + chips), not by rank: A (tiny base,
    13.9x) ranks ABOVE B (full base, 4.35x) and wears its low_base flag."""
    pct = _order(client, "uplift_pct_desc")
    assert pct.index("A") < pct.index("B")
    abs_ = _order(client, "uplift_abs_desc")
    assert abs_.index("B") < abs_.index("A")  # abs sort unchanged: B's absolute uplift dominates


def test_zero_uplift_sorts_as_value_not_missing(client):
    """A genuine 0.0% uplift is a real value — it must rank among numbers, not
    with the no-reval rows (the old `or float(\'-inf\')` treated 0.0 as absent)."""
    import sqlite3 as _sq
    import api.portfolio as _pf
    conn = _pf.get_connection()
    _seed_company(conn, "Z", npv_dfs=100.0, npv_spot=100.0, uplift_pct=0.0)
    # company with NO reval at all
    now = "2026-06-12T00:00:00+00:00"
    conn.execute("INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES ('N','NoReval',?,?)", (now, now))
    ncid = conn.execute("SELECT company_id FROM companies WHERE ticker='N'").fetchone()[0]
    npid = conn.execute("INSERT INTO projects (company_id, project_name, country, created_at) VALUES (?, 'Bare', 'Australia', ?)", (ncid, now)).lastrowid
    conn.execute("INSERT INTO resources (project_id, effective_date, commodity, resource_or_reserve, category, tonnes, created_at) VALUES (?, '2024-01-01','Au','Resource','Indicated',1000000,?)", (npid, now))
    conn.commit()
    pct = _order(client, "uplift_pct_desc")
    assert pct.index("Z") < pct.index("N")   # 0.0 ranks as a value; no-reval last


def test_low_base_flag_and_abs_present(client):
    a = _company(client, "A")["latest_revaluation"]
    assert a["low_base"] is True
    assert a["npv_uplift_abs"] is not None
    b = _company(client, "B")["latest_revaluation"]
    assert b["low_base"] is False


def test_company_study_label_from_single_study(client):
    """latest_study_stage/date must come from the SAME (max-date) study — not two
    independent MAX() aggregates pairing 'PFS' (alphabetical max) with another
    study's date (I3)."""
    import api.portfolio as _pf
    conn = _pf.get_connection()
    now = "2026-06-12T00:00:00+00:00"
    cid = conn.execute("SELECT company_id FROM companies WHERE ticker='A'").fetchone()[0]
    pid2 = conn.execute(
        "INSERT INTO projects (company_id, project_name, country, created_at) VALUES (?, 'Second', 'Australia', ?)",
        (cid, now)).lastrowid
    conn.execute(
        "INSERT INTO studies (project_id, study_stage, study_confidence_tier, study_date, post_tax_npv, reporting_currency) "
        "VALUES (?, 'Scoping', 'conceptual', '2025-11-01', 50.0, 'AUD')", (pid2,))
    conn.commit()
    data = client.get("/api/portfolio/companies").get_json()
    a = next(c for c in data["companies"] if c["ticker"] == "A")
    # Newest study across projects is the 2025 Scoping — stage AND date from it.
    assert a["latest_study_stage"] == "Scoping"
    assert a["latest_study_date"] == "2025-11-01"
