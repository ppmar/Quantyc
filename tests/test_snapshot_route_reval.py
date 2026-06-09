"""D2: snapshot reval is decoupled from the displayed study — real-route integration.

Stands up the real schema via init_db() and hits the HTTP route, so it catches future
changes to the reval SQL (unlike copied-SQL unit tests).
"""
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest


_N = "2024-01-01"  # any non-null timestamp for NOT NULL columns


def _seed(db_path: str):
    import db as dbmod
    dbmod.DB_PATH = Path(db_path)
    dbmod.init_db()
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    c.execute("INSERT INTO companies(ticker,name,first_seen_at,last_updated_at) VALUES('TST','Test Co',?,?)", (_N, _N))
    cid = c.execute("SELECT company_id FROM companies WHERE ticker='TST'").fetchone()[0]
    c.execute("INSERT INTO projects(company_id,project_name,stage,created_at) VALUES(?,?,?,?)", (cid, "Demo", "feasibility", _N))
    pid = c.execute("SELECT project_id FROM projects WHERE project_name='Demo'").fetchone()[0]
    c.execute("INSERT INTO project_commodities(project_id,commodity,is_primary) VALUES(?,?,1)", (pid, "Au"))
    # Revalued DFS (2024) + a LATER Scoping (2025): the displayed study is the Scoping.
    c.execute("INSERT INTO studies(study_id,project_id,study_stage,study_confidence_tier,study_date,post_tax_npv,discount_rate_pct,reporting_currency) VALUES(1,?,'DFS','definitive','2024-06-15',451,8,'AUD')", (pid,))
    c.execute("INSERT INTO studies(study_id,project_id,study_stage,study_confidence_tier,study_date,post_tax_npv,discount_rate_pct,reporting_currency) VALUES(2,?,'Scoping','conceptual','2025-03-01',1178,8,'AUD')", (pid,))
    c.execute("INSERT INTO commodity_prices(commodity,price_usd,unit,source,fetched_at) VALUES('Au',4300,'USD/oz','test','2026-01-01')")
    sp = c.execute("SELECT price_id FROM commodity_prices LIMIT 1").fetchone()[0]
    c.execute(
        """INSERT INTO revaluations(study_id,project_id,company_id,computed_at,commodity,
               price_dfs,price_spot,price_spot_id,annual_production,annual_production_unit,
               mine_life_years,discount_rate_pct,tax_rate_pct,annuity_factor,
               npv_dfs,npv_spot,npv_uplift,npv_uplift_pct,method_version,study_confidence_tier)
           VALUES(1,?,?,'2024-07-01','Au',2000,4300,?,250000,'oz',10,8,30,6.7,451,1083,632,1.4,'first_order_v3','definitive')""",
        (pid, cid, sp),
    )
    c.commit()
    c.close()
    return pid


@pytest.fixture
def client(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed(db_path)

    def _tc():
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

    with patch("api.snapshot.get_connection", _tc):
        from api.snapshot import bp
        from flask import Flask
        app = Flask(__name__)
        app.register_blueprint(bp)
        app.config["TESTING"] = True
        with app.test_client() as c:
            yield c


def test_reval_shown_when_latest_study_is_conceptual(client):
    """The DFS reval (+140%) must surface even though the latest study is a Scoping."""
    resp = client.get("/api/company/TST/snapshot")
    assert resp.status_code == 200
    proj = resp.get_json()["projects"][0]
    assert proj["study"]["study_type"] == "Scoping"
    assert proj["revaluation"] is not None
    assert proj["revaluation"]["study_confidence_tier"] == "definitive"
    assert abs(proj["revaluation"]["npv_uplift_pct"] - 1.4) < 1e-9
