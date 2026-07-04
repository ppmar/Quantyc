"""Shared DB setup for portfolio endpoint tests."""
import sqlite3
from datetime import datetime


def setup_test_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

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
            study_confidence_tier TEXT,
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
            payback_years REAL,
            extraction_method TEXT,
            extraction_model TEXT,
            tax_rate_pct REAL,
            needs_review INTEGER DEFAULT 0,
            review_reason TEXT
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
            cutoff_grade REAL,
            cutoff_grade_unit TEXT,
            attributable_contained_metal REAL,
            section TEXT,
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
        CREATE TABLE revaluations (
            revaluation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id INTEGER,
            project_id INTEGER,
            company_id INTEGER,
            computed_at TEXT,
            commodity TEXT,
            price_dfs REAL,
            price_spot REAL,
            price_spot_id INTEGER,
            fx_rate REAL,
            fx_rate_price_id INTEGER,
            annual_production REAL,
            annual_production_unit TEXT,
            mine_life_years REAL,
            discount_rate_pct REAL,
            tax_rate_pct REAL,
            annuity_factor REAL,
            npv_dfs REAL,
            npv_spot REAL,
            npv_uplift REAL,
            npv_uplift_pct REAL,
            method_version TEXT,
            warnings TEXT,
            study_confidence_tier TEXT
        );
    """)

    now = datetime.utcnow().isoformat()

    # Company: DEG (with active project)
    conn.execute(
        "INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES (?, ?, ?, ?)",
        ("DEG", "De Grey Mining Limited", now, now),
    )

    # Company: ZZZ (no active projects — should be excluded)
    conn.execute(
        "INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES (?, ?, ?, ?)",
        ("ZZZ", "Empty Co", now, now),
    )

    deg_id = conn.execute("SELECT company_id FROM companies WHERE ticker='DEG'").fetchone()[0]
    zzz_id = conn.execute("SELECT company_id FROM companies WHERE ticker='ZZZ'").fetchone()[0]

    # DEG project: Hemi
    conn.execute(
        "INSERT INTO projects (company_id, project_name, country, state, stage, stage_source, region, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (deg_id, "Hemi", "Australia", "Western Australia", "feasibility", "gemini_inferred", "Pilbara", now),
    )
    hemi_id = conn.execute("SELECT project_id FROM projects WHERE project_name='Hemi'").fetchone()[0]

    # ZZZ project: NoData (no studies/resources)
    conn.execute(
        "INSERT INTO projects (company_id, project_name, country, created_at) VALUES (?, ?, ?, ?)",
        (zzz_id, "NoData", "Australia", now),
    )

    # Commodities
    conn.execute(
        "INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (?, ?, ?)",
        (hemi_id, "Au", 1),
    )

    # Document for study
    conn.execute(
        "INSERT INTO documents (ticker, url, sha256, source, announcement_date, ingested_at, header, parse_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("DEG", "http://example.com/dfs.pdf", "sha-deg-dfs", "asx_api", "2024-08-15", now, "Hemi DFS Results", "parsed"),
    )
    doc_id = conn.execute("SELECT document_id FROM documents LIMIT 1").fetchone()[0]

    # Study
    conn.execute(
        "INSERT INTO studies (project_id, document_id, study_stage, study_confidence_tier, study_date, post_tax_npv, reporting_currency) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (hemi_id, doc_id, "DFS", "definitive", "2024-08-15", 2900.0, "AUD"),
    )

    # Resource
    conn.execute(
        "INSERT INTO resources (project_id, effective_date, commodity, resource_or_reserve, category, tonnes, grade, grade_unit, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (hemi_id, "2024-04-20", "Au", "Resource", "Indicated", 250000000, 1.3, "g/t", now),
    )

    # Stage inference audit
    conn.execute(
        "INSERT INTO project_stage_inferences (project_id, stage, stage_confidence, region, reasoning, evidence_json, inferred_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (hemi_id, "feasibility", "high", "Pilbara", "DFS completed Aug 2024", "{}", now),
    )

    # Company: LIT (Li2O DFS — active, but excluded by supported_only since
    # lithium isn't in {Au,Ag,Cu})
    conn.execute(
        "INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES (?, ?, ?, ?)",
        ("LIT", "Lithium Co", now, now),
    )
    lit_id = conn.execute("SELECT company_id FROM companies WHERE ticker='LIT'").fetchone()[0]
    conn.execute(
        "INSERT INTO projects (company_id, project_name, country, stage, created_at) VALUES (?, ?, ?, ?, ?)",
        (lit_id, "Brine A", "Australia", "feasibility", now),
    )
    brine_id = conn.execute("SELECT project_id FROM projects WHERE project_name='Brine A'").fetchone()[0]
    conn.execute(
        "INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (?, ?, ?)",
        (brine_id, "Li2O", 1),
    )
    conn.execute(
        "INSERT INTO studies (project_id, study_stage, study_confidence_tier, study_date, post_tax_npv, reporting_currency) VALUES (?, ?, ?, ?, ?, ?)",
        (brine_id, "DFS", "definitive", "2024-09-01", 500.0, "AUD"),
    )

    # Company: PRD (production — more advanced than feasibility)
    conn.execute(
        "INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES (?, ?, ?, ?)",
        ("PRD", "Producer Co", now, now),
    )
    prd_id = conn.execute("SELECT company_id FROM companies WHERE ticker='PRD'").fetchone()[0]
    conn.execute(
        "INSERT INTO projects (company_id, project_name, country, stage, created_at) VALUES (?, ?, ?, ?, ?)",
        (prd_id, "BigMine", "Australia", "production", now),
    )
    bigmine_id = conn.execute("SELECT project_id FROM projects WHERE project_name='BigMine'").fetchone()[0]
    conn.execute(
        "INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (?, ?, 1)",
        (bigmine_id, "Au"),
    )
    conn.execute(
        "INSERT INTO resources (project_id, effective_date, commodity, resource_or_reserve, category, tonnes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (bigmine_id, "2024-01-01", "Au", "Reserve", "Proved", 10000000, now),
    )

    # Company: EXP (exploration — less advanced than feasibility)
    conn.execute(
        "INSERT INTO companies (ticker, name, first_seen_at, last_updated_at) VALUES (?, ?, ?, ?)",
        ("EXP", "Explorer Co", now, now),
    )
    exp_id = conn.execute("SELECT company_id FROM companies WHERE ticker='EXP'").fetchone()[0]
    conn.execute(
        "INSERT INTO projects (company_id, project_name, country, stage, created_at) VALUES (?, ?, ?, ?, ?)",
        (exp_id, "Greenfield", "Australia", "exploration", now),
    )
    green_id = conn.execute("SELECT project_id FROM projects WHERE project_name='Greenfield'").fetchone()[0]
    conn.execute(
        "INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (?, ?, 1)",
        (green_id, "Au"),
    )
    conn.execute(
        "INSERT INTO resources (project_id, effective_date, commodity, resource_or_reserve, category, tonnes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (green_id, "2024-01-01", "Au", "Resource", "Inferred", 5000000, now),
    )

    conn.commit()
    return conn
