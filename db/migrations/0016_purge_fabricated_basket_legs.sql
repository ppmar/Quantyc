-- Corrective for 0015: an earlier 0015 backfilled multi-primary projects by copying the
-- study's single annual_production onto EVERY is_primary leg, fabricating by-product
-- volumes (I8 violation: CHN Gonneville got 7000 for Au-oz AND Cu-t AND Ni-t; HCH Costa
-- Fuego 95000 for both Au-oz and Cu-t). 0015 is now single-primary only, but the bad legs
-- (and the revaluations computed from them) already existed on deployed DBs.
--
-- Fabrication signature (v2, tightened): a study with TWO OR MORE is_primary=1 legs —
-- exactly what the buggy backfill produced (one primary leg per primary commodity).
-- The original predicate ("project is multi-primary AND study has no is_primary=0 leg")
-- false-positived on genuinely re-extracted SINGLE-leg studies of multi-primary
-- projects (AEE Tiris: one real U3O8 leg, purged on every boot because this migration
-- re-runs at startup). A real extraction writes exactly one is_primary=1 leg, so the
-- >= 2 predicate can never touch it. Idempotent.

DELETE FROM revaluation_legs
WHERE revaluation_id IN (
    SELECT r.revaluation_id FROM revaluations r
    WHERE r.study_id IN (
        SELECT study_id FROM study_commodities
        WHERE is_primary = 1 GROUP BY study_id HAVING COUNT(*) >= 2
    )
);

DELETE FROM revaluations
WHERE study_id IN (
    SELECT study_id FROM study_commodities
    WHERE is_primary = 1 GROUP BY study_id HAVING COUNT(*) >= 2
);

DELETE FROM study_commodities
WHERE study_id IN (
    SELECT study_id FROM study_commodities
    WHERE is_primary = 1 GROUP BY study_id HAVING COUNT(*) >= 2
);
