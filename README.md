# Test Automation Framework — HR Data Pipeline QA

Metadata-driven automated QA for a production-grade GCP HR data pipeline
(Workday RaaS NDJSON → GCS → Dataflow → PostgreSQL, bronze/silver/gold,
SCD Type 4, content-addressed idempotency).

## Quickstart (local, no GCP needed)

```bash
export HR_PG_DSN="postgresql://hatch:hatch@127.0.0.1/hrdemo"

# 1. apply migrations + seed the QA metadata
bash scripts/migrate.sh

# 2. unit tests (40 tests)
python3 -m pytest tests/unit -q

# 3. end-to-end QA: smoke suite, then full regression (20 cases)
python3 -m test_framework.runner --env local --suite smoke
python3 -m test_framework.runner --env local --suite regression
```

Reports land in `test_framework/reports/<run_id>.md`; every run, case, and
assertion is also persisted in `tf.*` tables.

## Layout

| Path | What |
|---|---|
| `pipeline/` | Beam/Dataflow pipeline: `core` (pure logic), `transforms` (Beam), `dbio` (Postgres I/O), `main` (Beam entrypoint), `launcher` (orchestrator). `Dockerfile` + `metadata.json` for the Flex Template. |
| `db/migrations/` | `001` schemas → `002` bronze → `003` silver (SCD4) → `004` gold views → `005` ops/audit → `006` merge + DQ procedures → `007` QA metadata schema. |
| `db/seeds/` | QA framework metadata: environments, pipeline, suites, 20 test cases. Re-runnable (`DO UPDATE`). |
| `test_framework/` | `runner` (orchestrator), `fixtures` (deterministic NDJSON generators), `storage` (local/GCS backends), `assertions` (SQL assertion engine), `reporting` (Markdown reports). |
| `tests/unit/` | pytest suite for core logic, fixtures, assertions. |
| `docs/` | `ARCHITECTURE.md`, `TEST_PLAN.md`, `OPERATIONS.md`. |

## How a file flows through the system

1. **Ingest** (`pipeline/launcher.ingest_file`): reads the NDJSON from `gs://`
   (or a local path in `local` mode), SHA-256 hashes the bytes, derives a
   content-addressed `file_id` (UUID5). Byte-identical replays short-circuit
   as `skipped_duplicate` — before any bronze write — with an audit row.
2. **Bronze** (Beam/Dataflow or local loader): one row per NDJSON line into
   `bronze.raw_worker_events`; malformed lines and rows without `Worker_ID`
   go to `bronze.raw_worker_rejects`. PK `(file_id, line_no)` makes bronze
   writes naturally idempotent.
3. **Silver** (`silver.apply_bronze_batch`, one transaction): SCD Type 4 —
   changed workers get a new `workers_current` version and the old version
   moves to `workers_history`; unchanged workers only advance
   `last_seen_as_of_date`; stale (out-of-order) files never regress silver.
   Unknown RaaS fields are preserved in `attributes JSONB`.
4. **Gold**: views for current dim, full history, headcount by department,
   terminations by month, and missing-from-latest-snapshot.
5. **Ops**: `ops.file_ingestions` (one row per content hash), `ops.pipeline_runs`,
   and an append-only `ops.audit_log` record every state transition.

## QA framework

No YAML, no config files. Test cases, fixture sequences, assertion SQL,
environments, and suites live in `tf.*` tables; results are persisted per
run/case/execution/assertion. The runner generates deterministic fixtures,
uploads them through the storage backend, orchestrates the pipeline, then
evaluates the stored SQL assertions with `{placeholders}` bound from the run
context. See `docs/TEST_PLAN.md`.

## GCP deployment

See `docs/OPERATIONS.md` for the Flex Template build, Secret Manager wiring,
IAM, networking (Private IP Cloud SQL), and the `gcp` environment row.
