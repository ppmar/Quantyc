ALTER TABLE studies ADD COLUMN reporting_currency TEXT;
ALTER TABLE studies ADD COLUMN discount_rate_pct REAL;
ALTER TABLE studies ADD COLUMN pre_tax_npv REAL;
ALTER TABLE studies ADD COLUMN aisc_per_unit REAL;
ALTER TABLE studies ADD COLUMN aisc_unit TEXT;
ALTER TABLE studies ADD COLUMN payback_years REAL;
ALTER TABLE studies ADD COLUMN extraction_method TEXT;
ALTER TABLE studies ADD COLUMN extraction_model TEXT;
