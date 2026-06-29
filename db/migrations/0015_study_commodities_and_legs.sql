-- Multi-commodity basket revaluation (first_order_v4).
-- study_commodities: canonical per-metal production input (primary + by-products).
--   One leg per payable metal. studies.annual_production stays as legacy = primary leg.
-- revaluation_legs: per-metal breakdown of a basket revaluation. The revaluations row
--   keeps the aggregate result + the primary leg in its per-metal columns (no reader
--   breaks); the full basket lives here.

CREATE TABLE IF NOT EXISTS study_commodities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    study_id INTEGER NOT NULL REFERENCES studies(study_id),
    commodity TEXT NOT NULL,
    annual_production REAL,
    annual_production_unit TEXT,
    recovery_pct REAL,
    is_primary INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_study_commodities_study ON study_commodities(study_id);

CREATE TABLE IF NOT EXISTS revaluation_legs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    revaluation_id INTEGER NOT NULL REFERENCES revaluations(revaluation_id),
    commodity TEXT NOT NULL,
    supported INTEGER NOT NULL,                 -- 1 = Au/Ag/Cu (valued), 0 = not modeled
    price_dfs REAL, price_spot REAL, price_spot_id INTEGER,
    annual_production REAL, annual_production_unit TEXT,
    delta_revenue_annual_usd REAL,              -- this leg's contribution (0 if unsupported)
    dfs_metal_revenue_usd REAL                  -- production x dfs_price, for coverage_pct
);
CREATE INDEX IF NOT EXISTS idx_revaluation_legs_reval ON revaluation_legs(revaluation_id);

-- Backfill one leg per existing study. Units match what pipeline.py derives today
-- (oz for Au/Ag, t for Cu and everything else) so v3 <-> v4 stays identical (I1).
-- Idempotent: app.py re-applies every migration on each startup, so the NOT EXISTS
-- guard stops this INSERT from duplicating legs on reboot.
INSERT INTO study_commodities (study_id, commodity, annual_production, annual_production_unit, recovery_pct, is_primary)
SELECT s.study_id, pc.commodity, s.annual_production,
       CASE WHEN pc.commodity IN ('Au','Ag') THEN 'oz' ELSE 't' END,
       s.recovery_pct, 1
FROM studies s
JOIN project_commodities pc ON pc.project_id = s.project_id AND pc.is_primary = 1
WHERE s.annual_production IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM study_commodities sc WHERE sc.study_id = s.study_id);
