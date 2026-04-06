-- ASX Junior Miner Valuation Pipeline — Database Schema

CREATE TABLE IF NOT EXISTS documents (
    id              TEXT PRIMARY KEY,   -- sha256 of url
    company_ticker  TEXT NOT NULL,
    doc_type        TEXT,               -- appendix_5b | resource_update | drill_results | study | capital_raise | annual_report | quarterly_report | other
    header          TEXT,               -- announcement title from ASX
    announcement_date DATE,
    url             TEXT,
    local_path      TEXT,
    parse_status    TEXT DEFAULT 'pending',  -- pending | done | failed | needs_review
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS companies (
    ticker          TEXT PRIMARY KEY,
    name            TEXT,
    primary_commodity TEXT,             -- gold | copper | lithium | silver | zinc | etc
    reporting_currency TEXT DEFAULT 'AUD',
    fiscal_year_end TEXT,              -- MM-DD
    updated_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS company_financials (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    effective_date  DATE NOT NULL,
    shares_basic    REAL,
    shares_fd       REAL,               -- fully diluted: basic + options + warrants + rights + convertibles
    cash_aud        REAL,
    debt_aud        REAL,
    convertibles_aud REAL,
    quarterly_burn  REAL,               -- cash used in operations + investing, last quarter
    cash_runway_months REAL,            -- derived: cash / quarterly_burn * 3
    last_raise_date DATE,
    last_raise_price REAL,
    last_raise_shares REAL,
    source_doc_id   TEXT,
    extraction_method TEXT,
    confidence      TEXT,               -- high | medium | low
    needs_review    BOOLEAN DEFAULT 0,
    FOREIGN KEY (ticker) REFERENCES companies(ticker),
    FOREIGN KEY (source_doc_id) REFERENCES documents(id)
);

CREATE TABLE IF NOT EXISTS projects (
    id              TEXT PRIMARY KEY,   -- ticker_projectname slug
    ticker          TEXT NOT NULL,
    project_name    TEXT,
    country         TEXT DEFAULT 'Australia',
    state           TEXT,               -- WA | QLD | NSW | SA | NT | VIC | TAS
    stage           TEXT,               -- concept | discovery | feasibility | development | production
    ownership_pct   REAL,
    royalty_type    TEXT,               -- NSR | GRR | stream | none
    royalty_rate    REAL,
    stream_flag     BOOLEAN DEFAULT 0,
    permitting_risk TEXT,               -- low | medium | high | critical
    jurisdiction_risk TEXT,             -- low | medium | high
    is_primary      BOOLEAN DEFAULT 1,  -- is this the company's main asset?
    source_doc_id   TEXT,
    updated_at      TIMESTAMP,
    FOREIGN KEY (ticker) REFERENCES companies(ticker),
    FOREIGN KEY (source_doc_id) REFERENCES documents(id)
);

CREATE TABLE IF NOT EXISTS project_commodities (
    project_id      TEXT NOT NULL,
    commodity       TEXT NOT NULL,      -- gold | silver | copper | lithium | zinc | etc
    is_primary      BOOLEAN DEFAULT 1,
    PRIMARY KEY (project_id, commodity),
    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS resources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id      TEXT NOT NULL,
    commodity       TEXT NOT NULL,
    effective_date  DATE,
    estimate_type   TEXT,               -- resource | reserve
    category        TEXT,               -- Measured | Indicated | Inferred | Proven | Probable | Total
    tonnes_mt       REAL,               -- million tonnes
    grade           REAL,
    grade_unit      TEXT,               -- g/t | % | ppm | Li2O%
    contained_metal REAL,
    contained_unit  TEXT,               -- koz | Moz | kt | Mlb | Mt
    attributable_contained REAL,        -- derived: contained_metal * ownership_pct
    cut_off_grade   REAL,
    source_doc_id   TEXT,
    extraction_method TEXT,
    confidence      TEXT,
    needs_review    BOOLEAN DEFAULT 0,
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (source_doc_id) REFERENCES documents(id)
);

CREATE TABLE IF NOT EXISTS studies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id      TEXT NOT NULL,
    study_stage     TEXT,               -- scoping | pfs | dfs | production
    study_date      DATE,
    mine_life_years REAL,
    annual_production REAL,
    production_unit TEXT,               -- koz/yr | kt/yr | etc
    recovery_pct    REAL,
    initial_capex_musd REAL,
    sustaining_capex_musd REAL,
    opex_per_unit   REAL,
    opex_unit       TEXT,               -- $/oz | $/t
    post_tax_npv_musd REAL,
    irr_pct         REAL,
    assumed_commodity_price REAL,
    assumed_price_unit TEXT,            -- $/oz | $/t | $/lb
    assumed_fx_audusd REAL,
    discount_rate_pct REAL,
    source_doc_id   TEXT,
    extraction_method TEXT,
    confidence      TEXT,
    needs_review    BOOLEAN DEFAULT 0,
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (source_doc_id) REFERENCES documents(id)
);

CREATE TABLE IF NOT EXISTS drill_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id      TEXT NOT NULL,
    hole_id         TEXT NOT NULL,       -- e.g. SDDSC200
    prospect        TEXT,                -- e.g. Apollo, Golden Dyke
    from_m          REAL,                -- interval start depth in metres
    to_m            REAL,                -- interval end depth in metres
    interval_m      REAL,                -- downhole interval length
    true_width_m    REAL,                -- estimated true width (null if not stated)
    au_gt           REAL,                -- gold grade g/t
    au_eq_gt        REAL,                -- gold-equivalent grade g/t (if poly-metallic)
    sb_pct          REAL,                -- antimony %
    cu_pct          REAL,                -- copper %
    ag_gt           REAL,                -- silver g/t
    other_element   TEXT,                -- any other element reported
    other_grade     REAL,
    other_unit      TEXT,
    is_including     BOOLEAN DEFAULT 0,  -- is this a sub-interval ("including") row?
    depth_from_surface REAL,             -- vertical depth estimate (null if not available)
    azimuth         REAL,
    dip             REAL,
    easting         REAL,
    northing        REAL,
    elevation       REAL,
    announcement_date DATE,
    source_doc_id   TEXT,
    extraction_method TEXT,
    confidence      TEXT,
    needs_review    BOOLEAN DEFAULT 0,
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (source_doc_id) REFERENCES documents(id)
);

CREATE TABLE IF NOT EXISTS macro_assumptions (
    date            DATE PRIMARY KEY,
    gold_spot_usd   REAL,
    copper_spot_usd REAL,
    lithium_spot_usd REAL,
    silver_spot_usd REAL,
    aud_usd         REAL,
    base_discount_rate REAL DEFAULT 0.08,
    updated_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_extractions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id     TEXT NOT NULL,
    field_name      TEXT NOT NULL,
    raw_value       TEXT,
    normalized_value REAL,
    unit            TEXT,
    extraction_method TEXT,             -- rule_based | llm | manual
    confidence      TEXT,               -- high | medium | low
    needs_review    BOOLEAN DEFAULT 0,
    reviewed        BOOLEAN DEFAULT 0,
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents(id)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_documents_ticker ON documents(company_ticker);
CREATE INDEX IF NOT EXISTS idx_documents_type ON documents(doc_type);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(parse_status);
CREATE INDEX IF NOT EXISTS idx_financials_ticker ON company_financials(ticker, effective_date);
CREATE INDEX IF NOT EXISTS idx_projects_ticker ON projects(ticker);
CREATE INDEX IF NOT EXISTS idx_resources_project ON resources(project_id);
CREATE INDEX IF NOT EXISTS idx_studies_project ON studies(project_id);
CREATE INDEX IF NOT EXISTS idx_staging_document ON staging_extractions(document_id);
CREATE INDEX IF NOT EXISTS idx_drill_project ON drill_results(project_id);
CREATE INDEX IF NOT EXISTS idx_drill_hole ON drill_results(hole_id);
CREATE INDEX IF NOT EXISTS idx_drill_doc ON drill_results(source_doc_id);
