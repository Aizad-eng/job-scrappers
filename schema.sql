-- Job Scraper v2.0 Database Schema
-- Run this for fresh PostgreSQL setup (or let SQLAlchemy create it automatically)

-- Actor Configurations (stores actor definitions)
CREATE TABLE IF NOT EXISTS actor_configs (
    id SERIAL PRIMARY KEY,
    actor_key VARCHAR(50) UNIQUE NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    actor_id VARCHAR(255) NOT NULL,
    default_timeout_minutes INTEGER DEFAULT 15,
    default_max_results INTEGER DEFAULT 100,
    input_schema JSON DEFAULT '[]',
    input_template JSON DEFAULT '{}',
    output_mapping JSON DEFAULT '{}',
    filter_config JSON DEFAULT '{}',
    clay_template JSON DEFAULT '{}',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_actor_configs_key ON actor_configs(actor_key);

-- Job Searches (user-created search configurations)
CREATE TABLE IF NOT EXISTS job_searches (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    actor_key VARCHAR(50) NOT NULL REFERENCES actor_configs(actor_key),
    cron_schedule VARCHAR(50) NOT NULL,
    apify_token VARCHAR(255),
    actor_inputs JSON DEFAULT '{}',
    filter_rules JSON DEFAULT '{}',
    clay_webhook_url TEXT NOT NULL,
    batch_size INTEGER DEFAULT 8,
    batch_interval_ms INTEGER DEFAULT 2000,
    is_active BOOLEAN DEFAULT TRUE,
    last_run_at TIMESTAMP,
    last_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_job_searches_actor ON job_searches(actor_key);

-- Job Runs (execution history)
CREATE TABLE IF NOT EXISTS job_runs (
    id SERIAL PRIMARY KEY,
    job_search_id INTEGER NOT NULL REFERENCES job_searches(id) ON DELETE CASCADE,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(50) DEFAULT 'running',
    jobs_found INTEGER DEFAULT 0,
    jobs_filtered INTEGER DEFAULT 0,
    jobs_sent INTEGER DEFAULT 0,
    error_message TEXT,
    apify_run_id VARCHAR(255),
    apify_dataset_id VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_job_runs_search ON job_runs(job_search_id);

-- Scraped Jobs (individual job records)
CREATE TABLE IF NOT EXISTS scraped_jobs (
    id SERIAL PRIMARY KEY,
    job_run_id INTEGER NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    job_id VARCHAR(255),
    raw_data JSON DEFAULT '{}',
    extracted_data JSON DEFAULT '{}',
    sent_to_clay BOOLEAN DEFAULT FALSE,
    filtered_out BOOLEAN DEFAULT FALSE,
    filter_reason VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_scraped_jobs_run ON scraped_jobs(job_run_id);
CREATE INDEX IF NOT EXISTS idx_scraped_jobs_job_id ON scraped_jobs(job_id);
