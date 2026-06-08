from pipeline.orchestrator import header_stage_tier, normalize_annual_production


def test_header_stage_tier():
    assert header_stage_tier("Minyari Scoping Study Update Presentation") == "conceptual"
    assert header_stage_tier("Fisher East Scoping Study") == "conceptual"
    assert header_stage_tier("Syama Underground Pre-Feasibility Delivers Major Boost") == "indicative"
    assert header_stage_tier("Kathleen Valley Lithium Project DFS Update") == "definitive"
    assert header_stage_tier("Investor Presentation") is None
    assert header_stage_tier(None) is None


def test_normalize_annual_production():
    assert normalize_annual_production(187.4, "koz")[0] == 187400.0
    assert normalize_annual_production(3.33, "Moz")[0] == 3330000.0
    assert normalize_annual_production(250000, "oz")[0] == 250000
    assert normalize_annual_production(36000, "kt")[0] == 36000000
    assert normalize_annual_production(36000, "t")[0] == 36000
    # unknown unit -> unchanged (heuristic stays downstream)
    assert normalize_annual_production(187.4, None)[0] == 187.4
    assert normalize_annual_production(187.4, "weird")[0] == 187.4
    # warning emitted only when scaled
    assert normalize_annual_production(187.4, "koz")[1] is not None
    assert normalize_annual_production(250000, "oz")[1] is None


def _mk_conn():
    import sqlite3
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE projects (project_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER, project_name TEXT, created_at TEXT);
    """)
    return c


def test_commodity_suffix_dedup_and_scope_preserved():
    from pipeline.orchestrator import _get_or_create_project
    c = _mk_conn()
    a = _get_or_create_project(c, 1, "Syama Gold")   # creates "Syama"
    b = _get_or_create_project(c, 1, "Syama")        # matches same
    assert a == b
    # scope word kept distinct
    u = _get_or_create_project(c, 1, "Syama Underground")
    assert u != a
    # stored canonical name has commodity stripped
    name = c.execute("SELECT project_name FROM projects WHERE project_id=?", (a,)).fetchone()["project_name"]
    assert name == "Syama"
