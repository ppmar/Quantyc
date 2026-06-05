-- Extraction-quality flags for studies, mirroring company_financials.needs_review.
-- The revaluation engine uses post_tax_npv as its base NPV, so a NULL/mislabelled
-- NPV or a missing tax rate silently corrupts every downstream valuation. These
-- columns surface suspect study extractions for human review without blocking.

ALTER TABLE studies ADD COLUMN needs_review INTEGER NOT NULL DEFAULT 0;
ALTER TABLE studies ADD COLUMN review_reason TEXT;
