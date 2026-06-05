-- Remaining-life revaluation fix.
-- The first-order revaluation annuity was computed over the FULL mine_life even
-- for producing mines, crediting price uplift on ounces already mined. To run the
-- annuity over remaining life we need a production-start date per project, and we
-- record the remaining life actually used on each revaluation row for transparency.

ALTER TABLE projects ADD COLUMN production_start_date TEXT;  -- ISO date of first/commercial production; NULL = not yet producing
ALTER TABLE revaluations ADD COLUMN remaining_life_years REAL;  -- life used for the annuity (== mine_life_years for developers)
