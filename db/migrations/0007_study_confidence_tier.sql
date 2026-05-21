-- Adds study_confidence_tier to studies and revaluations tables.
-- Backfill from study_stage.
-- Note: run AFTER 0005_revaluation_tables.sql

ALTER TABLE studies ADD COLUMN study_confidence_tier TEXT;

UPDATE studies
SET study_confidence_tier = CASE
    WHEN study_stage LIKE '%DFS%' OR study_stage = 'FFS' THEN 'definitive'
    WHEN study_stage LIKE '%PFS%' THEN 'indicative'
    WHEN study_stage IN ('Scoping', 'PEA') THEN 'conceptual'
    ELSE NULL
END
WHERE study_confidence_tier IS NULL;

ALTER TABLE revaluations ADD COLUMN study_confidence_tier TEXT;

UPDATE revaluations
SET study_confidence_tier = (
    SELECT s.study_confidence_tier FROM studies s WHERE s.study_id = revaluations.study_id
)
WHERE study_confidence_tier IS NULL;
