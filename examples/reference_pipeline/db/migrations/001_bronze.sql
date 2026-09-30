-- 002_bronze.sql
-- BRONZE = raw landing zone. One row per NDJSON line, stored as JSONB exactly
-- as it arrived from the Workday RaaS extract. Append-only; never updated.
-- Idempotency: PRIMARY KEY (file_id, line_no) + INSERT ... ON CONFLICT DO NOTHING,
-- plus the content-addressed file registry in ops.file_ingestions (same bytes
-- can never be ingested twice).

CREATE TABLE IF NOT EXISTS bronze.raw_worker_events (
    file_id      UUID        NOT NULL,  -- content-addressed: uuid5(sha256 of file bytes)
    file_name    TEXT        NOT NULL,  -- original file name, e.g. workers_20260929.ndjson
    line_no      INTEGER     NOT NULL,  -- 1-based line number inside the file
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    as_of_date   DATE        NOT NULL,  -- business as-of date of the extract
    worker_json  JSONB       NOT NULL,  -- the worker object, verbatim
    row_hash     TEXT        NOT NULL,  -- sha256 of the raw line (line identity)
    PRIMARY KEY (file_id, line_no)
);

CREATE INDEX IF NOT EXISTS ix_bronze_events_worker
    ON bronze.raw_worker_events ((worker_json ->> 'Worker_ID'));
CREATE INDEX IF NOT EXISTS ix_bronze_events_ingested
    ON bronze.raw_worker_events (ingested_at);

-- Dead-letter table: lines that could not be parsed as a worker JSON object,
-- or that carry no usable Worker_ID. The rest of the file still loads.
CREATE TABLE IF NOT EXISTS bronze.raw_worker_rejects (
    reject_id    BIGSERIAL   PRIMARY KEY,
    file_id      UUID        NOT NULL,
    file_name    TEXT        NOT NULL,
    line_no      INTEGER     NOT NULL,
    raw_line     TEXT,                 -- truncated to 8k chars by the loader
    error        TEXT        NOT NULL, -- machine-readable reason, e.g. invalid_json
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_bronze_rejects_file
    ON bronze.raw_worker_rejects (file_id);
