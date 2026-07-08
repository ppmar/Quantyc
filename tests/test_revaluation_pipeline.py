"""Integration tests for revaluation pipeline — synthetic DB fixtures."""
import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from revaluation.pipeline import revalue_study
from revaluation.math import RevaluationError


@pytest.fixture
def test_db():
    """In-memory DB with full schema for pipeline tests."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE companies (
            company_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL UNIQUE,
            name TEXT,
            reporting_currency TEXT DEFAULT 'AUD',
            fiscal_year_end TEXT,
            first_seen_at TEXT NOT NULL,
            last_updated_at TEXT NOT NULL
        );
        CREATE TABLE projects (
            project_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL REFERENCES companies(company_id),
            project_name TEXT NOT NULL,
            country TEXT,
            state TEXT,
            stage TEXT,
            ownership_pct REAL,
            production_start_date TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE project_commodities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(project_id),
            commodity TEXT NOT NULL,
            is_primary INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE resources (
            resource_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(project_id),
            commodity TEXT NOT NULL
        );
        CREATE TABLE studies (
            study_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(project_id),
            document_id INTEGER,
            study_stage TEXT,
            study_confidence_tier TEXT,
            header_tier TEXT,
            study_date TEXT,
            mine_life_years REAL,
            annual_production REAL,
            recovery_pct REAL,
            initial_capex REAL,
            sustaining_capex REAL,
            opex REAL,
            post_tax_npv REAL,
            irr_pct REAL,
            assumed_price_deck TEXT,
            assumed_fx REAL,
            reporting_currency TEXT,
            discount_rate_pct REAL,
            pre_tax_npv REAL,
            aisc_per_unit REAL,
            aisc_unit TEXT,
            payback_years REAL,
            extraction_method TEXT,
            extraction_model TEXT,
            tax_rate_pct REAL,
            needs_review INTEGER DEFAULT 0,
            review_reason TEXT
        );
        CREATE TABLE commodity_prices (
            price_id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity TEXT NOT NULL,
            price_usd REAL NOT NULL,
            unit TEXT NOT NULL,
            source TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        );
        CREATE TABLE revaluations (
            revaluation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            company_id INTEGER NOT NULL,
            computed_at TEXT NOT NULL,
            commodity TEXT NOT NULL,
            price_dfs REAL NOT NULL,
            price_spot REAL NOT NULL,
            price_spot_id INTEGER NOT NULL,
            fx_rate REAL,
            fx_rate_price_id INTEGER,
            annual_production REAL NOT NULL,
            annual_production_unit TEXT NOT NULL,
            mine_life_years REAL NOT NULL,
            remaining_life_years REAL,
            discount_rate_pct REAL NOT NULL,
            tax_rate_pct REAL NOT NULL,
            annuity_factor REAL NOT NULL,
            npv_dfs REAL NOT NULL,
            npv_spot REAL NOT NULL,
            npv_uplift REAL NOT NULL,
            npv_uplift_pct REAL NOT NULL,
            method_version TEXT NOT NULL,
            warnings TEXT,
            study_confidence_tier TEXT
        );
        CREATE TABLE study_commodities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id INTEGER NOT NULL,
            commodity TEXT NOT NULL,
            annual_production REAL,
            annual_production_unit TEXT,
            recovery_pct REAL,
            is_primary INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE revaluation_legs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            revaluation_id INTEGER NOT NULL,
            commodity TEXT NOT NULL,
            supported INTEGER NOT NULL,
            price_dfs REAL, price_spot REAL, price_spot_id INTEGER,
            annual_production REAL, annual_production_unit TEXT,
            delta_revenue_annual_usd REAL, dfs_metal_revenue_usd REAL
        );
        -- Mirror the prod path (orchestrator persists legs / 0015 backfills them): seed
        -- the primary study_commodities leg from project_commodities on every study insert.
        -- Tests set project_commodities BEFORE inserting the study, so the leg commodity is
        -- correct. Multi-leg baskets insert their own extra rows.
        CREATE TRIGGER seed_primary_study_commodity AFTER INSERT ON studies
        BEGIN
            INSERT INTO study_commodities (study_id, commodity, annual_production,
                annual_production_unit, recovery_pct, is_primary)
            SELECT NEW.study_id, pc.commodity, NEW.annual_production,
                   CASE WHEN pc.commodity IN ('Au','Ag') THEN 'oz' ELSE 't' END,
                   NEW.recovery_pct, 1
            FROM project_commodities pc
            WHERE pc.project_id = NEW.project_id AND pc.is_primary = 1
            LIMIT 1;
        END;
    """)

    now = datetime.now(timezone.utc).isoformat()

    # Insert test company, project, commodity
    conn.execute(
        "INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES (?, ?, ?, ?)",
        ("DEG", "De Grey Mining", now, now),
    )
    conn.execute(
        "INSERT INTO projects (company_id, project_name, country, state, stage, ownership_pct, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (1, "Hemi", "Australia", "WA", "feasibility", 1.0, now),
    )
    conn.execute(
        "INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (?, ?, ?)",
        (1, "Au", 1),
    )
    conn.commit()
    return conn


def _insert_gold_dfs(conn, post_tax_npv=985.0, annual_production=180000.0,
                     mine_life_years=10.0, discount_rate_pct=5.0,
                     tax_rate_pct=None, gold_price=1900.0):
    price_deck = json.dumps([{"commodity": "Au", "price": gold_price, "unit": "USD/oz"}])
    conn.execute("""
        INSERT INTO studies (
            project_id, study_stage, study_date,
            mine_life_years, annual_production, recovery_pct,
            post_tax_npv, discount_rate_pct, tax_rate_pct,
            assumed_price_deck, reporting_currency
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (1, "DFS", "2024-06-15", mine_life_years, annual_production,
          92.0, post_tax_npv, discount_rate_pct, tax_rate_pct, price_deck, "AUD"))
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


# ── End-to-end gold revaluation ──────────────────────────────────


@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_study_end_to_end_au(mock_yahoo, test_db):
    """Full pipeline: synthetic DFS -> mock spot -> revaluations row."""
    study_id = _insert_gold_dfs(test_db)

    # Yahoo AUDUSD=X returns USD per AUD (~0.6452). See invariant I4 in specs/spec_revaluation_aud_fx_fix.md.
    def mock_quote(symbol):
        if symbol == "GC=F":
            return Decimal("3520")
        elif symbol == "AUDUSD=X":
            return Decimal("0.6452")
        raise ValueError(f"unexpected symbol: {symbol}")

    mock_yahoo.side_effect = mock_quote

    reval_id = revalue_study(test_db, study_id)
    assert reval_id is not None

    row = test_db.execute(
        "SELECT * FROM revaluations WHERE revaluation_id = ?", (reval_id,)
    ).fetchone()
    assert row is not None
    assert row["commodity"] == "Au"
    assert row["price_dfs"] == 1900.0
    assert row["price_spot"] == 3520.0
    assert row["method_version"] == "first_order_v4"
    assert row["npv_spot"] > row["npv_dfs"]
    assert row["npv_uplift"] > 0
    assert row["npv_uplift_pct"] > 0
    # Hand-checked: with spot=3520, fx=0.6452, NPV_DFS=985 AUD M
    # ΔNPV_USD = 180,000 * (3520-1900) * 7.7217 * 0.70 / 1e6 = 1576.16 USD M
    # ΔNPV_AUD = 1576.16 / 0.6452                              = 2443.00 AUD M
    # NPV_spot = 985 + 2443.00                                 = 3428.00 AUD M
    assert abs(row["npv_spot"] - 3428.00) < 1.0
    assert abs(row["npv_uplift"] - 2443.00) < 1.0


# ── Silver Moz auto-correction ────────────────────────────────────


@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_study_silver_moz_scaled(mock_yahoo, test_db):
    """Silver production reported in Moz (5.0) must be scaled to absolute oz."""
    test_db.execute("UPDATE project_commodities SET commodity = 'Ag' WHERE project_id = 1")
    test_db.commit()

    price_deck = json.dumps([{"commodity": "Ag", "price": 30.0, "unit": "USD/oz"}])
    test_db.execute("""
        INSERT INTO studies (
            project_id, study_stage, study_date,
            mine_life_years, annual_production, recovery_pct,
            post_tax_npv, discount_rate_pct, tax_rate_pct,
            assumed_price_deck, reporting_currency
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (1, "DFS", "2024-06-15", 10.0, 5.0, 90.0, 200.0, 5.0, 30.0, price_deck, "USD"))
    test_db.commit()
    study_id = test_db.execute("SELECT last_insert_rowid()").fetchone()[0]

    mock_yahoo.side_effect = lambda s: Decimal("75") if s == "SI=F" else (_ for _ in ()).throw(ValueError(s))

    reval_id = revalue_study(test_db, study_id)
    assert reval_id is not None
    row = test_db.execute("SELECT * FROM revaluations WHERE revaluation_id = ?", (reval_id,)).fetchone()
    # 5.0 Moz -> 5,000,000 oz scaled
    assert row["annual_production"] == 5_000_000.0
    # ΔNPV_USD = 5e6 * (75-30) * 7.7217 * 0.70 / 1e6 = 1216.17 USD M; NPV_spot = 200 + 1216.17
    assert abs(row["npv_spot"] - 1416.17) < 1.0
    assert row["npv_uplift_pct"] > 5.0


# ── Commodity skip ────────────────────────────────────────────────


@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_study_skips_lithium(mock_yahoo, test_db):
    """Li2O-only project: no supported leg -> not_revaluable_no_supported_commodity (I5).
    No revaluation row is written."""
    mock_yahoo.side_effect = lambda s: Decimal("0.6452") if s == "AUDUSD=X" else (
        (_ for _ in ()).throw(ValueError(s)))
    test_db.execute("UPDATE project_commodities SET commodity = 'Li2O' WHERE project_id = 1")
    test_db.commit()

    study_id = _insert_gold_dfs(test_db)
    with pytest.raises(RevaluationError, match=r"not_revaluable_no_supported_commodity"):
        revalue_study(test_db, study_id)

    count = test_db.execute("SELECT COUNT(*) FROM revaluations").fetchone()[0]
    assert count == 0


# ── Missing fields ────────────────────────────────────────────────


@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_study_raises_on_missing_npv(mock_yahoo, test_db):
    """NULL post_tax_npv -> RevaluationError."""
    study_id = _insert_gold_dfs(test_db, post_tax_npv=None)

    with pytest.raises(RevaluationError, match="missing_fields.*post_tax_npv"):
        revalue_study(test_db, study_id)


# ── Nonexistent study ────────────────────────────────────────────


def test_revalue_study_nonexistent_raises(test_db):
    with pytest.raises(RevaluationError, match="study_not_found"):
        revalue_study(test_db, 9999)


# ── Tier gate: conceptual studies are never revalued (PR1) ─────────

@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_study_blocks_conceptual(mock_yahoo, test_db):
    """Scoping/conceptual study -> RevaluationError, no row, no spot fetch."""
    price_deck = json.dumps([{"commodity": "Au", "price": 3500.0, "unit": "USD/oz"}])
    test_db.execute("""
        INSERT INTO studies (
            project_id, study_stage, study_confidence_tier, study_date,
            mine_life_years, annual_production, recovery_pct,
            post_tax_npv, discount_rate_pct, tax_rate_pct,
            assumed_price_deck, reporting_currency
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (1, "Scoping", "conceptual", "2025-06-30", 12.0, 141000.0, 84.0,
          1178.0, 5.0, 30.0, price_deck, "USD"))
    test_db.commit()
    sid = test_db.execute("SELECT last_insert_rowid()").fetchone()[0]

    with pytest.raises(RevaluationError, match=r"not_revaluable_tier:conceptual"):
        revalue_study(test_db, sid)
    mock_yahoo.assert_not_called()
    assert test_db.execute("SELECT COUNT(*) FROM revaluations").fetchone()[0] == 0


def test_revalue_study_blocks_null_tier_scoping_stage(test_db):
    """NULL tier on a Scoping stage derives to conceptual and is blocked (I2)."""
    price_deck = json.dumps([{"commodity": "Au", "price": 3500.0, "unit": "USD/oz"}])
    test_db.execute("""
        INSERT INTO studies (project_id, study_stage, study_confidence_tier, study_date,
            mine_life_years, annual_production, recovery_pct, post_tax_npv,
            discount_rate_pct, assumed_price_deck, reporting_currency)
        VALUES (?, 'Scoping', NULL, '2025-06-30', 12.0, 141000.0, 84.0, 1178.0, 5.0, ?, 'USD')
    """, (1, price_deck))
    test_db.commit()
    sid = test_db.execute("SELECT last_insert_rowid()").fetchone()[0]
    with pytest.raises(RevaluationError, match=r"not_revaluable_tier:conceptual"):
        revalue_study(test_db, sid)


# ── AUD-denominated price deck converted to USD (BTR bug) ─────────

@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_aud_deck_converted_to_usd(mock_yahoo, test_db):
    """A$/oz deck must be FX-converted to USD before the spot delta, else an
    AUD deck is wrongly compared to USD spot (BTR: A$5000 vs US$ spot)."""
    mock_yahoo.side_effect = lambda s: (
        Decimal("4000") if s == "GC=F" else Decimal("0.66") if s == "AUDUSD=X"
        else (_ for _ in ()).throw(ValueError(s))
    )
    deck = json.dumps([{"commodity": "Au", "price": "5000", "unit": "AUD/oz"}])
    test_db.execute("""
        INSERT INTO studies (project_id, study_stage, study_confidence_tier, study_date,
            mine_life_years, annual_production, recovery_pct, post_tax_npv, pre_tax_npv,
            discount_rate_pct, tax_rate_pct, assumed_price_deck, reporting_currency)
        VALUES (?, 'DFS','definitive','2024-06-15', 10.0, 150000.0, 90.0, 316.0, 450.0,
                5.0, 30.0, ?, 'AUD')
    """, (1, deck))
    test_db.commit()
    sid = test_db.execute("SELECT last_insert_rowid()").fetchone()[0]
    rid = revalue_study(test_db, sid)
    row = test_db.execute(
        "SELECT price_dfs, npv_uplift_pct, warnings FROM revaluations WHERE revaluation_id=?",
        (rid,),
    ).fetchone()
    # 5000 AUD * 0.66 = 3300 USD/oz, vs 4000 spot -> POSITIVE uplift (not negative).
    assert abs(row["price_dfs"] - 3300.0) < 1.0
    assert row["npv_uplift_pct"] > 0
    assert "price_deck_aud_to_usd" in row["warnings"]


def test_revalue_blocks_conceptual_by_header(test_db):
    """LLM mislabelled a Scoping study as DFS (tier definitive), but the header
    says Scoping -> header_tier='conceptual' must block the reval (AZY bypass)."""
    price_deck = json.dumps([{"commodity": "Au", "price": 1800.0, "unit": "USD/oz"}])
    test_db.execute("""
        INSERT INTO studies (project_id, study_stage, study_confidence_tier, header_tier, study_date,
            mine_life_years, annual_production, recovery_pct, post_tax_npv, discount_rate_pct,
            tax_rate_pct, assumed_price_deck, reporting_currency)
        VALUES (?, 'Updated DFS', 'definitive', 'conceptual', '2024-06-15', 10.0, 150000.0, 90.0,
                300.0, 5.0, 30.0, ?, 'USD')
    """, (1, price_deck))
    test_db.commit()
    sid = test_db.execute("SELECT last_insert_rowid()").fetchone()[0]
    with pytest.raises(RevaluationError, match=r"not_revaluable_tier:conceptual_by_header"):
        revalue_study(test_db, sid)


@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_study_cu_kt_magnitude_net(mock_yahoo, test_db):
    """Cu production 45 (t) is an implausible kt mislabel -> scaled to 45000 t."""
    test_db.execute("UPDATE project_commodities SET commodity = 'Cu' WHERE project_id = 1")
    test_db.commit()
    price_deck = json.dumps([{"commodity": "Cu", "price": 3.5, "unit": "USD/lb"}])
    test_db.execute("""
        INSERT INTO studies (project_id, study_stage, study_date, mine_life_years,
            annual_production, recovery_pct, post_tax_npv, discount_rate_pct, tax_rate_pct,
            assumed_price_deck, reporting_currency)
        VALUES (?, 'DFS', '2024-06-15', 10.0, 45.0, 90.0, 500.0, 8.0, 30.0, ?, 'USD')
    """, (1, price_deck))
    test_db.commit()
    sid = test_db.execute("SELECT last_insert_rowid()").fetchone()[0]
    mock_yahoo.side_effect = lambda s: Decimal("4.5") if s == "HG=F" else (_ for _ in ()).throw(ValueError(s))
    rid = revalue_study(test_db, sid)
    row = test_db.execute("SELECT * FROM revaluations WHERE revaluation_id = ?", (rid,)).fetchone()
    assert row["annual_production"] == 45000.0
    assert row["annual_production_unit"] == "t"
    assert row["npv_uplift"] > 0


@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_study_cu_normal_t_not_scaled(mock_yahoo, test_db):
    """A normal Cu figure (45000 t) is left untouched by the net."""
    test_db.execute("UPDATE project_commodities SET commodity = 'Cu' WHERE project_id = 1")
    test_db.commit()
    price_deck = json.dumps([{"commodity": "Cu", "price": 3.5, "unit": "USD/lb"}])
    test_db.execute("""
        INSERT INTO studies (project_id, study_stage, study_date, mine_life_years,
            annual_production, recovery_pct, post_tax_npv, discount_rate_pct, tax_rate_pct,
            assumed_price_deck, reporting_currency)
        VALUES (?, 'DFS', '2024-06-15', 10.0, 45000.0, 90.0, 500.0, 8.0, 30.0, ?, 'USD')
    """, (1, price_deck))
    test_db.commit()
    sid = test_db.execute("SELECT last_insert_rowid()").fetchone()[0]
    mock_yahoo.side_effect = lambda s: Decimal("4.5") if s == "HG=F" else (_ for _ in ()).throw(ValueError(s))
    rid = revalue_study(test_db, sid)
    row = test_db.execute("SELECT * FROM revaluations WHERE revaluation_id = ?", (rid,)).fetchone()
    assert row["annual_production"] == 45000.0


def _cu_au_spot(symbol):
    if symbol == "GC=F":
        return Decimal("2600")
    if symbol == "HG=F":
        return Decimal("4.50")
    if symbol == "AUDUSD=X":
        return Decimal("0.6452")
    raise ValueError(symbol)


def _insert_basket_study(conn, deck, extra_legs):
    """Insert a definitive USD DFS for project 1 (primary Au, 100koz) and add extra
    study_commodities legs. The trigger already seeded the Au primary leg."""
    test_deck = json.dumps(deck)
    conn.execute("""
        INSERT INTO studies (project_id, study_stage, study_confidence_tier, study_date,
            mine_life_years, annual_production, recovery_pct, post_tax_npv, discount_rate_pct,
            tax_rate_pct, assumed_price_deck, reporting_currency)
        VALUES (1,'DFS','definitive','2024-06-15',10.0,100000.0,90.0,400.0,8.0,30.0,?, 'USD')
    """, (test_deck,))
    sid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for commodity, prod, unit in extra_legs:
        conn.execute(
            "INSERT INTO study_commodities (study_id, commodity, annual_production, "
            "annual_production_unit, is_primary) VALUES (?, ?, ?, ?, 0)",
            (sid, commodity, prod, unit))
    conn.commit()
    return sid


@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_cu_au_basket_full_coverage(mock_yahoo, test_db):
    """Cu-Au, both supported -> valued (no longer quarantined), coverage 100% (I5)."""
    mock_yahoo.side_effect = _cu_au_spot
    test_db.execute("INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (1, 'Cu', 0)")
    test_db.commit()
    deck = [{"commodity": "Au", "price": 1900.0, "unit": "USD/oz"},
            {"commodity": "Cu", "price": 3.5, "unit": "USD/lb"}]
    sid = _insert_basket_study(test_db, deck, [("Cu", 20000.0, "t")])
    rid = revalue_study(test_db, sid)

    row = test_db.execute("SELECT * FROM revaluations WHERE revaluation_id = ?", (rid,)).fetchone()
    assert row["method_version"] == "first_order_v4"
    warns = json.loads(row["warnings"])
    assert any(w == "coverage_pct:100.0" for w in warns)
    assert not any("partial_basket_coverage" in w for w in warns)

    legs = test_db.execute(
        "SELECT commodity, supported FROM revaluation_legs WHERE revaluation_id = ? ORDER BY commodity",
        (rid,)).fetchall()
    assert {(r["commodity"], r["supported"]) for r in legs} == {("Au", 1), ("Cu", 1)}


@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_cu_au_co_basket_partial_coverage(mock_yahoo, test_db):
    """Cu-Au-Co: Cu+Au valued, Co recorded as an unsupported leg, coverage < 100% (I3, I4)."""
    mock_yahoo.side_effect = _cu_au_spot
    for c in ("Cu", "Co"):
        test_db.execute("INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (1, ?, 0)", (c,))
    test_db.commit()
    deck = [{"commodity": "Au", "price": 1900.0, "unit": "USD/oz"},
            {"commodity": "Cu", "price": 3.5, "unit": "USD/lb"},
            {"commodity": "Co", "price": 15.0, "unit": "USD/lb"}]
    sid = _insert_basket_study(test_db, deck, [("Cu", 20000.0, "t"), ("Co", 1000.0, "t")])
    rid = revalue_study(test_db, sid)

    row = test_db.execute("SELECT * FROM revaluations WHERE revaluation_id = ?", (rid,)).fetchone()
    warns = json.loads(row["warnings"])
    assert any(w.startswith("partial_basket_coverage") for w in warns)

    legs = {r["commodity"]: r for r in test_db.execute(
        "SELECT * FROM revaluation_legs WHERE revaluation_id = ?", (rid,)).fetchall()}
    assert legs["Co"]["supported"] == 0
    assert legs["Co"]["delta_revenue_annual_usd"] == 0.0
    assert legs["Co"]["dfs_metal_revenue_usd"] > 0  # counts toward coverage denominator
    assert legs["Cu"]["supported"] == 1 and legs["Au"]["supported"] == 1


@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_ni_zn_no_supported_commodity(mock_yahoo, test_db):
    """Ni-Zn: no supported leg -> not_revaluable_no_supported_commodity (I5)."""
    mock_yahoo.side_effect = _cu_au_spot
    test_db.execute("UPDATE project_commodities SET commodity = 'Ni' WHERE project_id = 1")
    test_db.execute("INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (1, 'Zn', 0)")
    test_db.commit()
    deck = [{"commodity": "Ni", "price": 8.0, "unit": "USD/lb"},
            {"commodity": "Zn", "price": 1.2, "unit": "USD/lb"}]
    sid = _insert_basket_study(test_db, deck, [("Zn", 5000.0, "t")])  # primary leg seeded as Ni
    with pytest.raises(RevaluationError, match=r"not_revaluable_no_supported_commodity"):
        revalue_study(test_db, sid)


@patch("revaluation.prices.fetch_yahoo_quote")
def test_stale_study_warned_even_for_developer(mock_yahoo, test_db):
    """A >3y-old study gets stale_study on every reval (not just producers):
    old deck + old cost base = restudy needed, not signal (PRU/HAV/PNR class)."""
    mock_yahoo.side_effect = lambda s: {"GC=F": Decimal("4000"), "AUDUSD=X": Decimal("0.6452")}[s]
    sid = _insert_gold_dfs(test_db)
    test_db.execute("UPDATE studies SET study_date='2019-06-15' WHERE study_id=?", (sid,))
    test_db.commit()
    rid = revalue_study(test_db, sid)
    warns = json.loads(test_db.execute(
        "SELECT warnings FROM revaluations WHERE revaluation_id=?", (rid,)).fetchone()[0])
    assert any(w.startswith("stale_study:") for w in warns)
    # deck 1900 < 4000/2 -> divergence flag too
    assert any(w.startswith("deck_far_below_spot_Au") for w in warns)


@patch("revaluation.prices.fetch_yahoo_quote")
def test_producer_without_start_date_warned(mock_yahoo, test_db):
    """stage=production but production_start_date NULL -> no depletion applied,
    uplift overstated (PNR Norseman case). Must be flagged."""
    mock_yahoo.side_effect = lambda s: {"GC=F": Decimal("4000"), "AUDUSD=X": Decimal("0.6452")}[s]
    test_db.execute("UPDATE projects SET stage='production' WHERE project_id=1")
    test_db.commit()
    sid = _insert_gold_dfs(test_db)
    rid = revalue_study(test_db, sid)
    warns = json.loads(test_db.execute(
        "SELECT warnings FROM revaluations WHERE revaluation_id=?", (rid,)).fetchone()[0])
    assert "producer_missing_start_date_no_depletion_uplift_overstated" in warns


@patch("revaluation.prices.fetch_yahoo_quote")
def test_review_flagged_study_reval_carries_flag(mock_yahoo, test_db):
    """A needs_review study (e.g. post_tax == pre_tax NPV, BTR Menzies) still revalues
    but the result must carry study_needs_review — a flagged base is a weak signal."""
    mock_yahoo.side_effect = lambda s: {"GC=F": Decimal("4000"), "AUDUSD=X": Decimal("0.6452")}[s]
    sid = _insert_gold_dfs(test_db)
    test_db.execute(
        "UPDATE studies SET needs_review=1, review_reason='post_tax_npv_ge_pre_tax_npv' "
        "WHERE study_id=?", (sid,))
    test_db.commit()
    rid = revalue_study(test_db, sid)
    warns = json.loads(test_db.execute(
        "SELECT warnings FROM revaluations WHERE revaluation_id=?", (rid,)).fetchone()[0])
    assert "study_needs_review:post_tax_npv_ge_pre_tax_npv" in warns


@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_u3o8_end_to_end(mock_yahoo, test_db):
    """Uranium study revalues per lb via UX=F. Production stored as raw 3.6 (Mlb
    mislabel, legacy leg) -> magnitude net scales to 3,600,000 lb."""
    mock_yahoo.side_effect = lambda s: Decimal("84.25") if s == "UX=F" else (_ for _ in ()).throw(ValueError(s))
    test_db.execute("UPDATE project_commodities SET commodity = 'U3O8' WHERE project_id = 1")
    test_db.commit()
    price_deck = json.dumps([{"commodity": "U3O8", "price": 60.0, "unit": "USD/lb"}])
    test_db.execute("""
        INSERT INTO studies (project_id, study_stage, study_confidence_tier, study_date, mine_life_years,
            annual_production, recovery_pct, post_tax_npv, discount_rate_pct, tax_rate_pct,
            assumed_price_deck, reporting_currency)
        VALUES (?, 'DFS', 'definitive', '2025-06-15', 12.0, 3.6, 90.0, 341.0, 8.0, 30.0, ?, 'USD')
    """, (1, price_deck))
    test_db.commit()
    sid = test_db.execute("SELECT last_insert_rowid()").fetchone()[0]
    rid = revalue_study(test_db, sid)
    row = test_db.execute("SELECT * FROM revaluations WHERE revaluation_id = ?", (rid,)).fetchone()
    assert row["commodity"] == "U3O8"
    assert row["annual_production"] == 3_600_000.0
    assert row["annual_production_unit"] == "lb"
    assert row["price_spot"] == 84.25
    assert row["npv_uplift"] > 0
    warns = json.loads(row["warnings"])
    assert any("scaled_U3O8" in w for w in warns)


@patch("revaluation.prices.fetch_yahoo_quote")
def test_revalue_u3o8_ambiguous_band_refused(mock_yahoo, test_db):
    """A U3O8 figure in the 20-100k band (tonnes? klb?) is refused, never guessed."""
    mock_yahoo.side_effect = lambda s: Decimal("84.25") if s == "UX=F" else Decimal("0.65")
    test_db.execute("UPDATE project_commodities SET commodity = 'U3O8' WHERE project_id = 1")
    test_db.commit()
    price_deck = json.dumps([{"commodity": "U3O8", "price": 60.0, "unit": "USD/lb"}])
    test_db.execute("""
        INSERT INTO studies (project_id, study_stage, study_confidence_tier, study_date, mine_life_years,
            annual_production, recovery_pct, post_tax_npv, discount_rate_pct, tax_rate_pct,
            assumed_price_deck, reporting_currency)
        VALUES (?, 'DFS', 'definitive', '2025-06-15', 12.0, 460.0, 90.0, 341.0, 8.0, 30.0, ?, 'USD')
    """, (1, price_deck))
    test_db.commit()
    sid = test_db.execute("SELECT last_insert_rowid()").fetchone()[0]
    with pytest.raises(RevaluationError, match="u3o8_production_unit_ambiguous"):
        revalue_study(test_db, sid)


# ── Daily reval refresh ───────────────────────────────────────────


@patch("revaluation.prices.fetch_yahoo_quote")
def test_refresh_recomputes_stale_reval(mock_yahoo, test_db):
    """A latest-revaluable study whose newest reval is older than the cutoff gets a
    fresh row at current spot; a fresh one is left alone (no churn within a day)."""
    from revaluation.pipeline import refresh_stale_revaluations
    mock_yahoo.side_effect = lambda s: {"GC=F": Decimal("4000"), "AUDUSD=X": Decimal("0.6452")}[s]
    sid = _insert_gold_dfs(test_db)
    rid1 = revalue_study(test_db, sid)
    # Age the row -> stale -> refresh writes a second row
    test_db.execute("UPDATE revaluations SET computed_at='2026-06-12T00:00:00+00:00' WHERE revaluation_id=?", (rid1,))
    test_db.commit()
    mock_yahoo.side_effect = lambda s: {"GC=F": Decimal("4200"), "AUDUSD=X": Decimal("0.6452")}[s]
    # expire the cached spot rows too, else get_or_fetch_price reuses the 4000 print
    test_db.execute("UPDATE commodity_prices SET fetched_at='2026-06-12T00:00:00+00:00'")
    test_db.commit()
    stats = refresh_stale_revaluations(test_db)
    assert stats == {"refreshed": 1, "skipped_fresh": 0, "errors": 0}
    rows = test_db.execute(
        "SELECT price_spot FROM revaluations WHERE study_id=? ORDER BY computed_at", (sid,)).fetchall()
    assert len(rows) == 2 and rows[1]["price_spot"] == 4200.0
    # Second sweep: newest row is fresh -> nothing to do
    stats2 = refresh_stale_revaluations(test_db)
    assert stats2 == {"refreshed": 0, "skipped_fresh": 0, "errors": 0}


@patch("revaluation.prices.fetch_yahoo_quote")
def test_refresh_ranks_among_revalued_studies_only(mock_yahoo, test_db):
    """A newer study WITHOUT reval rows (npv-less, or auto-reval failed) must not
    block the project from the daily refresh (RMX: an npv-less 2016 PFS blocked the
    revalued studies entirely). The screen selects among reval rows, so refresh must
    keep the same study current."""
    from revaluation.pipeline import refresh_stale_revaluations
    mock_yahoo.side_effect = lambda s: {"GC=F": Decimal("4000"), "AUDUSD=X": Decimal("0.6452")}[s]
    old_sid = _insert_gold_dfs(test_db)                       # 2024-06-15, revalued
    rid = revalue_study(test_db, old_sid)
    test_db.execute("UPDATE revaluations SET computed_at='2026-06-12T00:00:00+00:00' WHERE revaluation_id=?", (rid,))
    # Newer study with NO reval rows (npv NULL -> never revaluable).
    test_db.execute("""
        INSERT INTO studies (project_id, study_stage, study_confidence_tier, study_date,
            mine_life_years, annual_production, recovery_pct, post_tax_npv, discount_rate_pct,
            tax_rate_pct, assumed_price_deck, reporting_currency)
        VALUES (1,'PFS','indicative','2025-10-01',10.0,150000.0,90.0,NULL,5.0,30.0,
                '[{"commodity":"Au","price":2500,"unit":"USD/oz"}]','AUD')
    """)
    test_db.execute("UPDATE commodity_prices SET fetched_at='2026-06-12T00:00:00+00:00'")
    test_db.commit()
    stats = refresh_stale_revaluations(test_db)
    assert stats["refreshed"] == 1  # the revalued study stays current
    n = test_db.execute("SELECT COUNT(*) FROM revaluations WHERE study_id=?", (old_sid,)).fetchone()[0]
    assert n == 2
