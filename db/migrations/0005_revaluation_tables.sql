-- Revaluation pipeline tables + tax_rate_pct on studies

CREATE TABLE IF NOT EXISTS commodity_prices (
    price_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity       TEXT    NOT NULL,         -- 'Au', 'Cu', 'AUDUSD' (FX as pseudo-commodity)
    price_usd       REAL    NOT NULL,
    unit            TEXT    NOT NULL,         -- 'USD/oz', 'USD/lb', 'AUD/USD'
    source          TEXT    NOT NULL,         -- 'yahoo:GC=F', 'yahoo:HG=F', 'manual'
    fetched_at      TEXT    NOT NULL          -- ISO timestamp
);

CREATE INDEX IF NOT EXISTS idx_prices_commodity_time
    ON commodity_prices(commodity, fetched_at DESC);

CREATE TABLE IF NOT EXISTS revaluations (
    revaluation_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    study_id                INTEGER NOT NULL REFERENCES studies(study_id),
    project_id              INTEGER NOT NULL REFERENCES projects(project_id),
    company_id              INTEGER NOT NULL REFERENCES companies(company_id),
    computed_at             TEXT    NOT NULL,
    commodity               TEXT    NOT NULL,
    price_dfs               REAL    NOT NULL,
    price_spot              REAL    NOT NULL,
    price_spot_id           INTEGER NOT NULL REFERENCES commodity_prices(price_id),
    fx_rate                 REAL,
    fx_rate_price_id        INTEGER REFERENCES commodity_prices(price_id),
    annual_production       REAL    NOT NULL,
    annual_production_unit  TEXT    NOT NULL,
    mine_life_years         REAL    NOT NULL,
    discount_rate_pct       REAL    NOT NULL,
    tax_rate_pct            REAL    NOT NULL,
    annuity_factor          REAL    NOT NULL,
    npv_dfs                 REAL    NOT NULL,
    npv_spot                REAL    NOT NULL,
    npv_uplift              REAL    NOT NULL,
    npv_uplift_pct          REAL    NOT NULL,
    method_version          TEXT    NOT NULL,
    warnings                TEXT
);

CREATE INDEX IF NOT EXISTS idx_revaluations_company
    ON revaluations(company_id, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_revaluations_uplift
    ON revaluations(npv_uplift_pct DESC);

ALTER TABLE studies ADD COLUMN tax_rate_pct REAL;
