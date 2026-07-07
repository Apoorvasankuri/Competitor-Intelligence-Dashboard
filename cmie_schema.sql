-- CMIE CapEx integration schema
-- Safe to run multiple times (uses IF NOT EXISTS throughout).
-- Does not touch any existing tables (processed_articles, users, sessions,
-- pipeline_runs, etc).

CREATE TABLE IF NOT EXISTS cmie_projects (
    id serial PRIMARY KEY,
    cmie_project_id text UNIQUE,
    project_name text,
    promoter_name text,
    project_cost numeric,
    project_status text,
    industry text,
    sector text,
    ownership text,
    state text,
    district text,
    location text,
    expected_completion text,
    latest_event_date text,
    latest_event text,
    raw_payload jsonb,
    last_synced_at timestamp DEFAULT now(),
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cmie_sync_runs (
    id serial PRIMARY KEY,
    sync_type text,
    setid text,
    batchid text,
    reporttype text,
    status text,
    records_in integer,
    records_upserted integer,
    error_message text,
    started_at timestamp DEFAULT now(),
    ended_at timestamp
);

CREATE INDEX IF NOT EXISTS idx_cmie_projects_project_id ON cmie_projects (cmie_project_id);
CREATE INDEX IF NOT EXISTS idx_cmie_projects_status ON cmie_projects (project_status);
CREATE INDEX IF NOT EXISTS idx_cmie_projects_state ON cmie_projects (state);
CREATE INDEX IF NOT EXISTS idx_cmie_projects_industry ON cmie_projects (industry);
CREATE INDEX IF NOT EXISTS idx_cmie_projects_promoter ON cmie_projects (promoter_name);
CREATE INDEX IF NOT EXISTS idx_cmie_sync_runs_started_at ON cmie_sync_runs (started_at);
