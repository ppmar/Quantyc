-- Persist parse-time extraction warnings on the study row.
-- These are schema-level warnings produced by StudyExtraction at parse time
-- (NPV post==pre / post>pre, malformed AISC currency unit). Distinct from
-- studies.needs_review (0010), which is computed at persist from final numbers.

ALTER TABLE studies ADD COLUMN extraction_warnings TEXT;  -- JSON array, nullable
