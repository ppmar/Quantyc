-- Migration 0002: Add JORC columns to resources + indexes
-- ALTER TABLE ADD COLUMN is a no-op if column already exists in newer schema

-- Indexes (always safe to re-run)
CREATE INDEX IF NOT EXISTS idx_resources_project_id ON resources(project_id);
CREATE INDEX IF NOT EXISTS idx_resources_document_id ON resources(document_id);
CREATE INDEX IF NOT EXISTS idx_resources_commodity ON resources(commodity);
CREATE INDEX IF NOT EXISTS idx_projects_company_id ON projects(company_id);
