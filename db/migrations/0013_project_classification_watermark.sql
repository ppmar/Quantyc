-- Watermark for the stage-classification process (Younes' last_time_classified).
-- last_classified_doc_id: highest documents.document_id the project was last classified
--   against. Monotonic trigger for re-classification. NULL = never classified.
-- last_classified_doc_date: announcement_date of that document, for display only.
ALTER TABLE projects ADD COLUMN last_classified_doc_id   INTEGER;
ALTER TABLE projects ADD COLUMN last_classified_doc_date TEXT;
