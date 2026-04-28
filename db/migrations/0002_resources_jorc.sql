-- Migration 0002: Add JORC columns to resources + indexes + provenance
-- ALTER TABLE ADD COLUMN is a no-op if column already exists in newer schema

-- Indexes (always safe to re-run)
CREATE INDEX IF NOT EXISTS idx_resources_project_id ON resources(project_id);
CREATE INDEX IF NOT EXISTS idx_resources_document_id ON resources(document_id);
CREATE INDEX IF NOT EXISTS idx_resources_commodity ON resources(commodity);
CREATE INDEX IF NOT EXISTS idx_resources_effective_date ON resources(effective_date);
CREATE INDEX IF NOT EXISTS idx_projects_company_id ON projects(company_id);
CREATE INDEX IF NOT EXISTS idx_project_commodities_project ON project_commodities(project_id);
