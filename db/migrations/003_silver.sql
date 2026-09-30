-- 003_silver.sql
-- SILVER = curated zone, SCD Type 4.
--   silver.workers_current : exactly one row per worker_id = the latest version.
--   silver.workers_history : every superseded version, with valid_from/valid_to.
-- "Type 4" here = current + history kept in separate tables (fast current-state
-- reads, full audit trail on the side), as opposed to Type 2's single table
-- with effective-date flags.
--
-- Merge semantics (implemented in silver.apply_bronze_batch, migration 006):
--   * new worker_id            -> INSERT into workers_current (version 1)
--   * same worker, hash differs, file as_of >= current as_of
--                              -> move current row to history, INSERT new version
--   * same worker, hash equal  -> no-op (only last_seen_as_of_date advances)
--   * file as_of < current as_of (stale/out-of-order file)
--                              -> row ignored for silver; bronze keeps the raw line
-- Terminations are versioned like any other change (status flips to
-- 'Terminated'); workers are never physically deleted.

CREATE TABLE IF NOT EXISTS silver.workers_current (
    worker_id           TEXT        PRIMARY KEY,  -- Workday Worker_ID, natural key
    employee_id         TEXT,
    first_name          TEXT,
    last_name           TEXT,
    preferred_name      TEXT,
    email               TEXT,
    hire_date           DATE,
    termination_date    DATE,
    worker_status       TEXT,                     -- e.g. Active / Terminated / Leave
    job_profile         TEXT,
    job_family          TEXT,
    department          TEXT,
    department_id       TEXT,
    location            TEXT,
    country             TEXT,
    manager_worker_id   TEXT,
    cost_center         TEXT,
    employment_type     TEXT,
    worker_type         TEXT,
    time_type           TEXT,
    compensation_grade  TEXT,
    annual_salary       NUMERIC(14,2),
    currency            TEXT,
    attributes          JSONB       NOT NULL DEFAULT '{}',  -- passthrough for unknown/new RaaS fields (schema-drift safe)
    record_hash         TEXT        NOT NULL,  -- sha256 over the canonical business fields; drives change detection
    source_file_id      UUID        NOT NULL,  -- bronze lineage
    source_line_no      INTEGER     NOT NULL,  -- bronze lineage
    as_of_date          DATE        NOT NULL,  -- business as-of of the version
    last_seen_as_of_date DATE       NOT NULL,  -- newest as_of this worker appeared in (drives "missing from snapshot" detection)
    valid_from          TIMESTAMPTZ NOT NULL,  -- system time this version became current
    version             INTEGER     NOT NULL DEFAULT 1,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_silver_current_status
    ON silver.workers_current (worker_status);
CREATE INDEX IF NOT EXISTS ix_silver_current_department
    ON silver.workers_current (department);
CREATE INDEX IF NOT EXISTS ix_silver_current_email
    ON silver.workers_current (email);

CREATE TABLE IF NOT EXISTS silver.workers_history (
    history_id          BIGSERIAL   PRIMARY KEY,
    worker_id           TEXT        NOT NULL,
    employee_id         TEXT,
    first_name          TEXT,
    last_name           TEXT,
    preferred_name      TEXT,
    email               TEXT,
    hire_date           DATE,
    termination_date    DATE,
    worker_status       TEXT,
    job_profile         TEXT,
    job_family          TEXT,
    department          TEXT,
    department_id       TEXT,
    location            TEXT,
    country             TEXT,
    manager_worker_id   TEXT,
    cost_center         TEXT,
    employment_type     TEXT,
    worker_type         TEXT,
    time_type           TEXT,
    compensation_grade  TEXT,
    annual_salary       NUMERIC(14,2),
    currency            TEXT,
    attributes          JSONB       NOT NULL DEFAULT '{}',
    record_hash         TEXT        NOT NULL,
    source_file_id      UUID        NOT NULL,
    source_line_no      INTEGER     NOT NULL,
    as_of_date          DATE        NOT NULL,
    last_seen_as_of_date DATE       NOT NULL,
    valid_from          TIMESTAMPTZ NOT NULL,  -- when this version became current
    valid_to            TIMESTAMPTZ NOT NULL,  -- when it was superseded
    version             INTEGER     NOT NULL,
    superseded_by_file_id UUID,                -- the file whose version replaced this one
    archived_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_silver_history_worker
    ON silver.workers_history (worker_id, version);
CREATE INDEX IF NOT EXISTS ix_silver_history_valid
    ON silver.workers_history (valid_from, valid_to);
