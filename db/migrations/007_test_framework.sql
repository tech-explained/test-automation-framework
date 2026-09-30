-- 007_test_framework.sql
-- TF = test-framework metadata. The runner reads EVERYTHING from these tables:
-- no YAML, no JSON config files, no CLI-baked test lists. "Metadata-driven"
-- means: to add a test case you INSERT a row; to disable one you flip a flag.
-- "Persistence-driven" means: every run, step, assertion and artifact is
-- written back here, so the report is just a query and the audit trail is
-- complete by construction.

-- Execution environments (local dev vs real GCP).
CREATE TABLE IF NOT EXISTS tf.environments (
    env_id            TEXT PRIMARY KEY,   -- 'local' | 'gcp'
    display_name      TEXT NOT NULL,
    pipeline_mode     TEXT NOT NULL DEFAULT 'local'
        CHECK (pipeline_mode IN ('local', 'dataflow')),
    storage_backend   TEXT NOT NULL DEFAULT 'local'
        CHECK (storage_backend IN ('local', 'gcs')),
    gcp_project       TEXT,
    gcs_bucket        TEXT,
    local_bucket_root TEXT,               -- used when storage_backend='local'
    dataflow_region   TEXT NOT NULL DEFAULT 'us-central1',
    db_dsn_env_var    TEXT NOT NULL DEFAULT 'HR_PG_DSN',
    active            BOOLEAN NOT NULL DEFAULT TRUE
);

-- Pipelines under test (lets the framework version them independently).
CREATE TABLE IF NOT EXISTS tf.pipelines (
    pipeline_id            TEXT PRIMARY KEY,  -- e.g. 'hr-workday-ndjson-v1'
    display_name           TEXT NOT NULL,
    flex_template_gcs_path TEXT,              -- null until published to GCS
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
CREATE TABLE IF NOT EXISTS tf.test_cases (
    test_case_id     TEXT PRIMARY KEY,   -- 'TC-001'
    name             TEXT NOT NULL,
    category         TEXT NOT NULL,      -- functional|idempotency|scd4|negative|edge|audit|dq
    description      TEXT NOT NULL,
    executions       INTEGER NOT NULL DEFAULT 1,
    fixture_sequence JSONB NOT NULL,
    pipeline_id      TEXT NOT NULL REFERENCES tf.pipelines(pipeline_id),
    expectations     JSONB NOT NULL DEFAULT '{"assertions": []}',
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
