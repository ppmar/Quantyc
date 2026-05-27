-- Adds region field and provenance metadata for the stage column.
-- Also creates project_stage_inferences audit table.

ALTER TABLE projects ADD COLUMN region TEXT;
ALTER TABLE projects ADD COLUMN stage_source TEXT;
ALTER TABLE projects ADD COLUMN stage_inferred_at TEXT;

-- Backfill provenance for existing OZMIN-loaded rows.
UPDATE projects
SET stage_source = 'ozmin'
WHERE stage IS NOT NULL AND stage_source IS NULL;

CREATE INDEX IF NOT EXISTS idx_projects_stage ON projects(stage);
CREATE INDEX IF NOT EXISTS idx_projects_stage_source ON projects(stage_source);

-- Audit table for Gemini classification reasoning.
CREATE TABLE IF NOT EXISTS project_stage_inferences (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id    INTEGER NOT NULL REFERENCES projects(project_id),
    stage         TEXT,
    stage_confidence TEXT,
    region        TEXT,
    reasoning     TEXT,
    evidence_json TEXT NOT NULL,
    inferred_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_psi_project ON project_stage_inferences(project_id, inferred_at DESC);
