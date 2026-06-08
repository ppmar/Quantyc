-- Robustness audit (R1, R2).
-- header_tier: stage derived from the document header; the reval gate uses it to
--   override a mislabelled LLM study_type toward conceptual (never revalue a
--   Scoping study the LLM called "DFS").
-- annual_production_unit: the source unit, so production is normalized to a
--   canonical absolute value at persist instead of a fragile magnitude heuristic.
ALTER TABLE studies ADD COLUMN annual_production_unit TEXT;
ALTER TABLE studies ADD COLUMN header_tier TEXT;
