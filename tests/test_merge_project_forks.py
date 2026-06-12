"""Fork merge: same company + same normalized name → one project row.

Keeper = lowest project_id; children (studies, revaluations,
project_commodities, resources, stage inferences) repointed. A repointed
study that collides with the keeper's unique (project_id, stage, npv) dedup
index is itself a duplicate — it and its revaluations are deleted.
"""
from datetime import datetime, timezone

import pytest

from db import get_connection, init_db
from scripts.merge_project_forks import merge_forks


@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("db.DB_PATH", db_path)
    init_db()
    yield


def _seed():
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO companies (ticker, first_seen_at, last_updated_at) VALUES ('IVR', ?, ?)",
        (now, now),
    )
    cid = cur.lastrowid
    p1 = conn.execute(
        "INSERT INTO projects (company_id, project_name, created_at) VALUES (?, 'Paris', ?)",
        (cid, now),
    ).lastrowid
    p2 = conn.execute(
        "INSERT INTO projects (company_id, project_name, created_at) VALUES (?, 'Paris Silver', ?)",
        (cid, now),
    ).lastrowid
    # distinct studies on each fork
    s1 = conn.execute(
        "INSERT INTO studies (project_id, study_stage, post_tax_npv, reporting_currency, discount_rate_pct) VALUES (?, 'DFS', 445.0, 'AUD', 8.0)",
        (p1,),
    ).lastrowid
    s2 = conn.execute(
        "INSERT INTO studies (project_id, study_stage, post_tax_npv, reporting_currency, discount_rate_pct) VALUES (?, 'DFS', 832.0, 'AUD', 8.0)",
        (p2,),
    ).lastrowid
    # colliding duplicate on the fork (same stage+npv as keeper's study)
    s_dup = conn.execute(
        "INSERT INTO studies (project_id, study_stage, post_tax_npv, reporting_currency, discount_rate_pct) VALUES (?, 'DFS', 445.0, 'AUD', 8.0)",
        (p2,),
    ).lastrowid
    conn.execute(
        "INSERT INTO commodity_prices (commodity, price_usd, unit, source, fetched_at)"
        " VALUES ('Ag', 66.0, 'USD/oz', 'test', ?)",
        (now,),
    )
    price_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """INSERT INTO revaluations (study_id, project_id, company_id, computed_at, commodity,
           price_dfs, price_spot, price_spot_id, annual_production, annual_production_unit,
           mine_life_years, discount_rate_pct, tax_rate_pct, annuity_factor,
           npv_dfs, npv_spot, npv_uplift, npv_uplift_pct, method_version)
           VALUES (?, ?, ?, ?, 'Ag', 24.0, 66.0, ?, 2700000, 'oz',
                   9.0, 8.0, 30.0, 6.25, 445.0, 900.0, 455.0, 1.02, 't')""",
        (s_dup, p2, cid, now, price_id),
    )
    conn.execute(
        "INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (?, 'Ag', 1)", (p1,)
    )
    conn.execute(
        "INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (?, 'Ag', 1)", (p2,)
    )
    conn.commit()
    return conn, p1, p2, s1, s2, s_dup


def test_forks_merged_to_keeper():
    conn, p1, p2, s1, s2, s_dup = _seed()
    stats = merge_forks(conn, dry_run=False)

    assert stats["groups_merged"] == 1
    # fork row gone, keeper remains
    names = [r[0] for r in conn.execute("SELECT project_name FROM projects").fetchall()]
    assert names == ["Paris"]

    # distinct study repointed to keeper
    assert conn.execute(
        "SELECT project_id FROM studies WHERE study_id = ?", (s2,)
    ).fetchone()[0] == p1

    # colliding duplicate study + its revaluations removed
    assert conn.execute(
        "SELECT COUNT(*) FROM studies WHERE study_id = ?", (s_dup,)
    ).fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM revaluations").fetchone()[0] == 0

    # commodities deduped on the keeper
    assert conn.execute(
        "SELECT COUNT(*) FROM project_commodities WHERE project_id = ?", (p1,)
    ).fetchone()[0] == 1


def test_dry_run_changes_nothing():
    conn, p1, p2, *_ = _seed()
    stats = merge_forks(conn, dry_run=True)
    assert stats["groups_merged"] == 1
    assert conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0] == 2
