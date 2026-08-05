-- CMIE CapEx: Civil Highlights feature migration
-- Safe to run multiple times.
-- Adds a dedicated "last updated" field distinct from latest_event_date,
-- since CMIE's own "Last Updated" column (if present in your set/batch data)
-- may not be the same as the most recent tracked event date.

ALTER TABLE cmie_projects ADD COLUMN IF NOT EXISTS last_updated_date text;

CREATE INDEX IF NOT EXISTS idx_cmie_projects_last_updated
    ON cmie_projects (last_updated_date);
