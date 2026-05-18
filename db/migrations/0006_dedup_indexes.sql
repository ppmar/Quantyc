-- Prevent duplicate projects and studies from repeated syncs/extractions
CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_company_name ON projects (company_id, project_name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_studies_dedup ON studies (project_id, study_stage, post_tax_npv);
