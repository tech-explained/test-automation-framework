-- 001_test_framework.sql
-- Test Automation Framework metadata schema (tf.*).
--
-- This is the ENTIRE framework database: the runner reads everything from
-- these tables (no YAML, no JSON config files, no CLI-baked test lists).
-- "Metadata-driven" means: to add a test case you INSERT a row; to disable
-- one you flip a flag. "Persistence-driven" means: every run, step,
-- assertion and artifact is written back here, so the report is just a
-- query and the audit trail is complete by construction.
--
-- The framework NEVER creates tables for the pipeline under test. Your
-- pipeline's own schema (bronze/silver/gold, staging/marts, whatever it
-- is) lives with your pipeline — see examples/reference_pipeline/db/ for
-- how the reference example does it.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS tf;

-- Execution environments. The pipeline under test is ALWAYS external: it
-- plugs in via ingest_adapter, a dotted path 'module.path:function_name'
-- implementing the ingest contract documented in
-- test_framework/pipeline_adapter.py (see examples/adapter_template.py).
CREATE TABLE IF NOT EXISTS tf.environments (
    env_id            TEXT PRIMARY KEY,   -- 'local' | 'gcp'
    display_name      TEXT NOT NULL,
    pipeline_mode     TEXT NOT NULL DEFAULT 'external'
        CHECK (pipeline_mode = 'external'),
    storage_backend   TEXT NOT NULL DEFAULT 'local'
        CHECK (storage_backend IN ('local', 'gcs')),
    gcp_project       TEXT,
    gcs_bucket        TEXT,
    local_bucket_root TEXT,               -- used when storage_backend='local'
    region            TEXT,               -- cloud region, if applicable
    db_dsn_env_var    TEXT NOT NULL DEFAULT 'HR_PG_DSN',
    ingest_adapter    TEXT,               -- e.g. 'mycompany.qa_adapter:ingest_file'
    active            BOOLEAN NOT NULL DEFAULT TRUE
);

COMMENT ON COLUMN tf.environments.ingest_adapter IS
    'Dotted path module.path:function_name implementing the pipeline ingest '
    'contract (see test_framework/pipeline_adapter.py and '
    'examples/adapter_template.py).';

-- Pipelines under test (lets the framework version them independently).
CREATE TABLE IF NOT EXISTS tf.pipelines (
    pipeline_id            TEXT PRIMARY KEY,  -- e.g. 'sample-pipeline-v1'
    display_name           TEXT NOT NULL,
    launch_config          JSONB,              -- pipeline-specific launch settings for your adapter
    pipeline_version       TEXT NOT NULL DEFAULT 'v1',
    active                 BOOLEAN NOT NULL DEFAULT TRUE
);

-- Named suites: ordered lists of test-case ids. The runner takes --suite.
CREATE TABLE IF NOT EXISTS tf.suites (
    suite_id     TEXT PRIMARY KEY,   -- 'smoke' | 'regression' | ...
    display_name TEXT NOT NULL,
    test_case_ids JSONB NOT NULL DEFAULT '[]',
    active       BOOLEAN NOT NULL DEFAULT TRUE
);

-- Test cases. fixture_sequence = ordered list of
--   {"generator": "<name>", "params": {...}, "as_of_date": "YYYY-MM-DD"}
-- The runner ingests each file in order, then evaluates `expectations`.
-- executions > 1 replays the whole sequence (idempotency tests).
-- expectations = {"assertions": [ {"name","kind","sql","op","expected"}, ... ]}
-- SQL may use {placeholders}: {prefix}, {file_id}, {file_id_0..N},
-- {as_of_0..N}, {target_worker}. The runner binds them as query params.
-- batchable = FALSE for cases whose value depends on per-file load
-- isolation (poison fixtures, replay tests, the empty-file case): they
-- always ingest solo, even in --ingest-mode batch.
CREATE TABLE IF NOT EXISTS tf.test_cases (
    test_case_id     TEXT PRIMARY KEY,   -- 'TC-001'
    name             TEXT NOT NULL,
    category         TEXT NOT NULL,      -- functional|idempotency|scd4|negative|edge|audit|dq
    description      TEXT NOT NULL,
    executions       INTEGER NOT NULL DEFAULT 1,
    fixture_sequence JSONB NOT NULL,
    pipeline_id      TEXT NOT NULL REFERENCES tf.pipelines(pipeline_id),
    expectations     JSONB NOT NULL DEFAULT '{"assertions": []}',
    batchable        BOOLEAN NOT NULL DEFAULT TRUE,
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    owner            TEXT NOT NULL DEFAULT 'qa-framework'
);

-- One row per framework run.
CREATE TABLE IF NOT EXISTS tf.test_runs (
    run_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id       TEXT NOT NULL REFERENCES tf.environments(env_id),
    suite        TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running','passed','failed','error')),
    triggered_by TEXT NOT NULL DEFAULT 'qa-framework',
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    summary      JSONB NOT NULL DEFAULT '{}'
);

-- One row per (run, test case, execution). Inserted BEFORE execution so a
-- crashed runner still leaves a trace; updated to terminal status after.
CREATE TABLE IF NOT EXISTS tf.test_case_results (
    run_id        UUID NOT NULL REFERENCES tf.test_runs(run_id),
    test_case_id  TEXT NOT NULL REFERENCES tf.test_cases(test_case_id),
    execution_no  INTEGER NOT NULL DEFAULT 1,
    status        TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running','passed','failed','error','skipped')),
    gcs_uris      JSONB NOT NULL DEFAULT '[]',
    file_ids      JSONB NOT NULL DEFAULT '[]',
    load_statuses JSONB NOT NULL DEFAULT '[]',
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    details       JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (run_id, test_case_id, execution_no)
);

-- One row per assertion evaluation. expected/actual are JSONB so the report
-- can show exactly what differed.
CREATE TABLE IF NOT EXISTS tf.assertion_results (
    assertion_id BIGSERIAL PRIMARY KEY,
    run_id       UUID NOT NULL,
    test_case_id TEXT NOT NULL,
    execution_no INTEGER NOT NULL DEFAULT 1,
    name         TEXT NOT NULL,
    kind         TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('pass','fail','error')),
    expected     JSONB,
    actual       JSONB,
    message      TEXT,
    checked_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_assertion_run
    ON tf.assertion_results (run_id, test_case_id);
