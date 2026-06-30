-- Corrective for 0015: an earlier 0015 backfilled multi-primary projects by copying the
-- study's single annual_production onto EVERY is_primary leg, fabricating by-product
-- volumes (I8 violation: CHN Gonneville got 7000 for Au-oz AND Cu-t AND Ni-t; HCH Costa
-- Fuego 95000 for both Au-oz and Cu-t). 0015 is now single-primary only, but the bad legs
-- (and the revaluations computed from them) already exist on deployed DBs. Purge them so
-- those studies fall back to "no legs → not revalued" until a real re-extraction supplies
-- per-metal commodity_production. Single-primary legs are untouched.
--
-- "Fabricated" = a study_commodities backfill row whose project has >1 is_primary commodity.
-- Re-extracted baskets are distinguishable: they carry non-primary (is_primary=0) legs, so
-- only purge studies that have NO is_primary=0 leg (i.e. pure backfill, never re-extracted).

DELETE FROM revaluation_legs
WHERE revaluation_id IN (
    SELECT r.revaluation_id FROM revaluations r
    WHERE r.study_id IN (
        SELECT sc.study_id FROM study_commodities sc
        JOIN studies s ON s.study_id = sc.study_id
        WHERE (SELECT COUNT(*) FROM project_commodities pc
               WHERE pc.project_id = s.project_id AND pc.is_primary = 1) > 1
          AND NOT EXISTS (SELECT 1 FROM study_commodities sc0
                          WHERE sc0.study_id = sc.study_id AND sc0.is_primary = 0)
    )
);

DELETE FROM revaluations
WHERE study_id IN (
    SELECT sc.study_id FROM study_commodities sc
    JOIN studies s ON s.study_id = sc.study_id
    WHERE (SELECT COUNT(*) FROM project_commodities pc
           WHERE pc.project_id = s.project_id AND pc.is_primary = 1) > 1
      AND NOT EXISTS (SELECT 1 FROM study_commodities sc0
                      WHERE sc0.study_id = sc.study_id AND sc0.is_primary = 0)
);

DELETE FROM study_commodities
WHERE study_id IN (
    SELECT sc.study_id FROM study_commodities sc
    JOIN studies s ON s.study_id = sc.study_id
    WHERE (SELECT COUNT(*) FROM project_commodities pc
           WHERE pc.project_id = s.project_id AND pc.is_primary = 1) > 1
)
  AND study_id NOT IN (
    SELECT sc0.study_id FROM study_commodities sc0 WHERE sc0.is_primary = 0
);
