# Architecture

## Data flow

```
Workday RaaS ── NDJSON ──▶ GCS bucket ──▶ Dataflow (Flex Template, Beam)
                                │                    │
                                │              bronze.raw_worker_events
                                │              bronze.raw_worker_rejects
                                │                    │  (plpgsql, 1 txn)
                                │                    ▼
                                │              silver.apply_bronze_batch()
                                │               ├─ workers_current (SCD4)
                                │               └─ workers_history (SCD4)
                                │                    │
                                │                    ▼
                                │              gold.* views (consumption)
                                │
                         ops.file_ingestions / ops.pipeline_runs / ops.audit_log
```

`pipeline/launcher.ingest_file` is the single orchestrator for both modes:

- **local** — parses NDJSON in-process and writes bronze directly (used by the
  QA framework; no GCP needed).
- **dataflow** — launches the Flex Template with the file parameters, polls to
  a terminal state, then runs the same silver merge + DQ as local mode.

The Beam path (`pipeline/main.py` + `transforms.py`) does bronze loading only;
the SCD4 merge stays in PostgreSQL (`006_silver_merge.sql`) so both modes
share one implementation — no duplicated merge logic to drift apart.

## Why SCD Type 4

HR worker records change constantly (department moves, promotions,
terminations, rehires) and point-in-time questions are the norm ("who was in
Data Platform on Sept 1?"). SCD4 keeps `workers_current` fast for the common
case (one row per worker) while `workers_history` retains every superseded
version with `valid_from`/`valid_to`. Terminations are versioned, never
physically deleted.

## Idempotency (three layers)

1. **Content-addressed files**: `file_id = UUID5(SHA-256(bytes))`. A
   byte-identical replay is detected *before any work* and short-circuits as
   `skipped_duplicate` (with an audit row). A concurrent `processing` file
   raises instead of double-loading.
2. **Bronze PK** `(file_id, line_no)`: re-running a failed load's bronze
   write is `ON CONFLICT DO NOTHING`-safe.
3. **Business-hash merge**: silver compares canonical record hashes, so a
   reordered file (same business data, new bytes) produces zero new versions —
   only `last_seen_as_of_date` advances.

## Schema-drift tolerance

RaaS extracts gain/lose fields without notice. The merge promotes a known
field list into typed silver columns; *everything else* is kept verbatim in
`attributes JSONB`. New fields never break the load; promoting a new field to
a typed column is a DDL + merge-procedure change, not a pipeline rewrite.

## Auditability

- `ops.file_ingestions`: one row per content hash — rows received/loaded/
  rejected, SCD counters, DQ warnings, status timeline.
- `ops.pipeline_runs`: one row per launcher run, linked to the ingestion.
- `ops.audit_log`: append-only; every transition (`ingest.started`,
  `bronze.loaded`, `silver.merge.completed`, `ingest.skipped_duplicate`, …)
  with actor, entity, and JSONB details.
- Lineage: every silver row carries `source_file_id` + `source_line_no`
  back to the exact bronze line.

## Reliability

- Malformed JSON, non-object lines, and missing-`Worker_ID` rows are
  quarantined in `bronze.raw_worker_rejects` — the file still completes.
- DQ checks (`ops.compute_dq_warnings`) are advisory: empty file, high
  null-email/department ratios, invalid dates/salaries, duplicate workers.
  Warnings are recorded on the ingestion row; the load is never blocked.
- Out-of-order (stale) snapshots are detected by `as_of_date` and counted,
  never applied.
- In-file duplicate `Worker_ID`s resolve deterministically: last line wins,
  counted in the audit details.

## Secrets

The Dataflow workers never see the Postgres DSN. The Flex Template takes
`pg_dsn_secret` — a Secret Manager **secret name** — and resolves it at
runtime (`dbio.dsn_from_secret`). Locally, `HR_PG_DSN` carries the DSN.
