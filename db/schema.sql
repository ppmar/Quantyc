-- Quantyc Lean Schema — Week 1+2
-- All timestamps are ISO-8601 text. All money in reporting currency.

-- ─────────────────────────────────────────────────────────────
-- documents: every PDF ever ingested (manual upload or ASX poll)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS documents (
    document_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker             TEXT    NOT NULL,
    url                TEXT    NOT NULL,
    sha256             TEXT    NOT NULL UNIQUE,    -- sha256("ticker:url")
    source             TEXT    NOT NULL,           -- 'asx_api' | 'manual_upload'
    announcement_date  TEXT,                       -- ISO date from ASX
    ingested_at        TEXT    NOT NULL,
    doc_type           TEXT,                       -- classified type
    header             TEXT,                       -- announcement headline
    parse_status       TEXT    NOT NULL DEFAULT 'pending',
                                                   -- pending|classified|parsed|failed|skipped
    parse_error        TEXT,
    local_path         TEXT    NOT NULL DEFAULT '' -- always empty, stateless
);
CREATE INDEX IF NOT EXISTS idx_documents_ticker ON documents(ticker);
CREATE INDEX IF NOT EXISTS idx_documents_type   ON documents(doc_type);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(parse_status);
CREATE INDEX IF NOT EXISTS idx_documents_sha256 ON documents(sha256);

-- ─────────────────────────────────────────────────────────────
-- companies: one row per ASX ticker
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS companies (
    company_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker             TEXT    NOT NULL UNIQUE,
    name               TEXT,
    reporting_currency TEXT    DEFAULT 'AUD',
    fiscal_year_end    TEXT,                       -- e.g. '06-30'
    first_seen_at      TEXT    NOT NULL,
    last_updated_at    TEXT    NOT NULL
);

-- ─────────────────────────────────────────────────────────────
-- company_financials: point-in-time capital structure snapshots
-- One row per (company, effective_date). Never overwrite; append.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS company_financials (
    financial_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id         INTEGER NOT NULL REFERENCES companies(company_id),
    document_id        INTEGER NOT NULL REFERENCES documents(document_id),
    effective_date     TEXT    NOT NULL,           -- period-end of the filing
    announcement_date  TEXT    NOT NULL,
    shares_basic       REAL,
    shares_fd          REAL,                       -- fully diluted
    options_outstanding REAL,
    perf_rights_outstanding REAL,
    convertibles_face_value REAL,
    cash               REAL,
    debt               REAL,
    quarterly_opex_burn REAL,                      -- from Appendix 5B section 1
    quarterly_invest_burn REAL,                    -- section 2
    extraction_method  TEXT    NOT NULL,           -- 'rule'|'llm'|'manual'
    confidence         TEXT    NOT NULL,           -- 'high'|'medium'|'low'
    needs_review       INTEGER NOT NULL DEFAULT 0, -- 0/1
    review_reason      TEXT,
    reviewed_at        TEXT,                       -- ISO timestamp of human override
    created_at         TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cf_company_date ON company_financials(company_id, effective_date DESC);
CREATE INDEX IF NOT EXISTS idx_cf_review       ON company_financials(needs_review);
CREATE INDEX IF NOT EXISTS idx_cf_document     ON company_financials(document_id);

-- ─────────────────────────────────────────────────────────────
-- Staging table for Appendix 5B extraction (Week 2)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS _stg_appendix_5b (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id        INTEGER NOT NULL REFERENCES documents(document_id),
    effective_date     TEXT,
    cash               REAL,
    debt               REAL,
    quarterly_opex_burn REAL,
    quarterly_invest_burn REAL,
    raw_json           TEXT,                       -- full extracted table JSON
    extraction_method  TEXT    NOT NULL DEFAULT 'rule',
    created_at         TEXT    NOT NULL,
    UNIQUE(document_id)
);

-- ─────────────────────────────────────────────────────────────
-- Staging table for issue-of-securities extraction (Week 2)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS _stg_issue_of_securities (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id        INTEGER NOT NULL REFERENCES documents(document_id),
    effective_date     TEXT,
    security_class     TEXT,                       -- 'ordinary' | 'option' | 'performance_right'
    quantity           REAL,
    total_on_issue     REAL,
    exercise_price     REAL,
    raw_json           TEXT,
    extraction_method  TEXT    NOT NULL DEFAULT 'rule',
    created_at         TEXT    NOT NULL,
    UNIQUE(document_id, security_class)
);

-- ─────────────────────────────────────────────────────────────
-- Placeholders for later weeks — create empty tables only.
-- Do not populate. Do not extract into them yet.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS projects (
    project_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id    INTEGER NOT NULL REFERENCES companies(company_id),
    project_name  TEXT    NOT NULL,
    country       TEXT,
    state         TEXT,
    stage         TEXT,
    ownership_pct REAL,
    created_at    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS project_commodities (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id   INTEGER NOT NULL REFERENCES projects(project_id),
    commodity    TEXT    NOT NULL,
    is_primary   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS resources (
    resource_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id        INTEGER NOT NULL REFERENCES projects(project_id),
    document_id       INTEGER REFERENCES documents(document_id),
    effective_date    TEXT    NOT NULL,
    commodity         TEXT    NOT NULL,
    resource_or_reserve TEXT  NOT NULL,
    category          TEXT    NOT NULL,            -- Measured|Indicated|Inferred|Proven|Probable
    tonnes            REAL,
    grade             REAL,
    contained_metal   REAL,
    attributable_contained_metal REAL,
    created_at        TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS studies (
    study_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id     INTEGER NOT NULL REFERENCES projects(project_id),
    document_id    INTEGER REFERENCES documents(document_id),
    study_stage    TEXT,                           -- scoping|PFS|DFS
    study_date     TEXT,
    mine_life_years REAL,
    annual_production REAL,
    recovery_pct   REAL,
    initial_capex  REAL,
    sustaining_capex REAL,
    opex           REAL,
    post_tax_npv   REAL,
    irr_pct        REAL,
    assumed_price_deck TEXT,                       -- JSON blob
    assumed_fx     REAL
);

CREATE TABLE IF NOT EXISTS valuations (
    valuation_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id     INTEGER NOT NULL REFERENCES companies(company_id),
    run_date       TEXT    NOT NULL,
    method         TEXT    NOT NULL,
    enterprise_value REAL,
    ev_per_attributable_unit REAL,
    project_nav    REAL,
    risked_nav     REAL,
    per_share_basic REAL,
    per_share_fd   REAL,
    funding_gap    REAL
);
