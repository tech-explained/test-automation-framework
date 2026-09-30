-- 005_ops.sql
-- OPS = ingestion registry + run tracking + append-only audit log.
-- This is the backbone of idempotency and auditability.

-- One row per distinct file CONTENT (file_id = uuid5(sha256)). A byte-identical
-- file can never be ingested twice: the launcher checks this table first and
-- returns 'skipped_duplicate' without touching bronze/silver.
CREATE TABLE IF NOT EXISTS ops.file_ingestions (
    file_id          UUID        PRIMARY KEY,
    file_name        TEXT        NOT NULL,
    gcs_uri          TEXT        NOT NULL,
    file_sha256      TEXT        NOT NULL UNIQUE,
    as_of_date       DATE        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'received'
        CHECK (status IN ('received','processing','completed','failed','skipped_duplicate')),
    rows_received    INTEGER     NOT NULL DEFAULT 0,  -- lines read from the file
    rows_loaded      INTEGER     NOT NULL DEFAULT 0,  -- lines landed in bronze
    rows_rejected    INTEGER     NOT NULL DEFAULT 0,  -- lines quarantined
    workers_upserted INTEGER     NOT NULL DEFAULT 0,  -- silver current inserts+updates
    history_rows_added INTEGER   NOT NULL DEFAULT 0,  -- silver history inserts
    workers_skipped_stale INTEGER NOT NULL DEFAULT 0,-- ignored: older as_of than current
    dq_warnings      JSONB       NOT NULL DEFAULT '[]', -- data-quality findings, non-blocking
    error            TEXT,                             -- failure reason when status='failed'
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_ingestions_status ON ops.file_ingestions (status);
CREATE INDEX IF NOT EXISTS ix_ingestions_asof   ON ops.file_ingestions (as_of_date);

-- Every pipeline execution (Dataflow job or local run) linked to its file.
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id        UUID        REFERENCES ops.file_ingestions(file_id),
    dataflow_job_id TEXT,                        -- null for local/direct runs
    mode           TEXT        NOT NULL,          -- 'dataflow' | 'local'
    status         TEXT        NOT NULL,          -- running|completed|failed
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    details        JSONB       NOT NULL DEFAULT '{}'
);

-- Append-only audit log. Writers: launcher, merge procedure, test framework.
-- Nothing here is ever updated or deleted.
CREATE TABLE IF NOT EXISTS ops.audit_log (
    audit_id  BIGSERIAL   PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor     TEXT        NOT NULL,  -- e.g. 'launcher', 'silver.merge', 'test-framework:<run_id>'
    action    TEXT        NOT NULL,  -- e.g. 'ingest.started', 'silver.merge.completed'
    entity    TEXT,                  -- e.g. 'file'
    entity_id TEXT,                  -- e.g. file_id
    details   JSONB       NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS ix_audit_entity ON ops.audit_log (entity, entity_id);
CREATE INDEX IF NOT EXISTS ix_audit_ts     ON ops.audit_log (ts);

-- Data-quality watchlist: workers present in an older snapshot but absent from
-- the latest completed one. Never auto-terminates anyone; surfaces for HR review.
CREATE OR REPLACE VIEW gold.vw_missing_from_latest_snapshot AS
WITH latest AS (
    SELECT max(as_of_date) AS d
    FROM ops.file_ingestions
    WHERE status = 'completed'
)
SELECT c.worker_id, c.preferred_name, c.email, c.department, c.worker_status,
       c.last_seen_as_of_date, l.d AS latest_snapshot_as_of
FROM silver.workers_current c
CROSS JOIN latest l
WHERE l.d IS NOT NULL
  AND c.last_seen_as_of_date < l.d;
