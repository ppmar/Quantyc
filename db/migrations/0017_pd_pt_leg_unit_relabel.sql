-- Pd/Pt study_commodities legs were written with the generic 't' label while their
-- values are troy ounces (verbatim LLM unit 'koz', already x1000-normalized by
-- normalize_annual_production — CHN Gonneville: Pd 197,000, Pt 17,000). Now that
-- Pd/Pt are supported commodities (basis oz), relabel. Values untouched. Idempotent.
UPDATE study_commodities SET annual_production_unit = 'oz'
WHERE commodity IN ('Pd', 'Pt') AND annual_production_unit = 't';
