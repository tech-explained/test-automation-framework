# Test Automation Framework

Metadata-driven QA for **any** data pipeline. You give the framework one
function — *load this file through my pipeline* — and it generates
deterministic fixtures, loads them through your pipeline, and evaluates
stored SQL assertions against your database. No YAML, no config drift: test
cases, environments, suites, and every result live in PostgreSQL (`tf.*`).

## The one seam: the ingest adapter

The framework never touches your pipeline's internals. It calls one function
per fixture file:

```python
def ingest_file(uri, *, file_name, as_of_date, env, actor) -> dict:
    """Load the fixture file at `uri` through the pipeline under test.

    MUST block until the load is finished — the runner evaluates assertions
    immediately after this returns.
    Returns {"file_id": str, "status": "completed"|"failed"|"skipped_duplicate",
             "error": str | None}.
    `file_id` feeds the {file_id} assertion placeholder: use whatever file
    identity your pipeline has (registry id, load id, ...).
    """
```

Point `tf.environments.ingest_adapter` at the dotted path of your adapter
(`module.path:function_name`). A complete working example is at
`examples/reference_adapter.py`.

## Quickstart (local, no GCP needed)

```bash
export HR_PG_DSN="postgresql://hatch:hatch@127.0.0.1/hrdemo"

# 1. apply the framework migrations (tf.* tables only; idempotent)
bash scripts/migrate.sh

# 2. apply the reference example: its own schema (bronze/silver/gold/ops),
#    seed data (environments, suites, 23-case QA matrix), and JSON-spec cases
bash examples/reference_pipeline/db/apply.sh

# 2. unit tests
python3 -m pytest tests/unit -q

# 3. end-to-end QA: smoke suite, then full regression (23 cases)
python3 -m test_framework.runner --env local --suite smoke
python3 -m test_framework.runner --env local --suite regression

# 4. same regression in batch ingest mode (fewer pipeline launches)
python3 -m test_framework.runner --env local --suite regression --ingest-mode batch
```

Reports land in `test_framework/reports/<run_id>.md`; every run, case, and
assertion is also persisted in `tf.*` tables.

## Layout

| Path | What |
|---|---|
| `test_framework/` | The framework: `runner` (orchestrator), `fixtures` (deterministic NDJSON generators), `storage` (local/GCS backends), `pipeline_adapter` (the external-only adapter seam), `assertions` (SQL assertion engine), `batching` (batch ingest planner), `reporting` (Markdown reports). Requirements: `psycopg`, `google-cloud-storage`. |
| `db/migrations/` | Framework schema only: `tf.*` tables (`001_test_framework.sql`). Your pipeline's tables live with your pipeline. |
| `db/seeds/` | Empty by design — the framework ships no seed data. Sample seeds live with the reference example. |
| `examples/` | **Not the framework.** `reference_pipeline/` is a working HR pipeline; `reference_pipeline/db/` holds its own schema (`bronze`/`silver`/`gold`/`ops`), seeds (environments, suites, 23 test cases), and `apply.sh`. `reference_adapter.py` implements the ingest contract against it. |
| `tests/unit/` | pytest suite for the framework: fixtures, assertions, batching, adapter (46 tests). |
| `fixtures/samples/` | JSON test-case specs consumed by `test_framework/add_case.py`. |
| `deploy/runner/` | Cloud Run Job packaging for running the framework itself on GCP. |
| `deploy/composer/` | Cloud Composer DAG that triggers the runner job. |
| `docs/` | `ARCHITECTURE.md`, `TEST_PLAN.md`, `OPERATIONS.md`. |

## How a run flows

1. **Plan** — the runner resolves the cases in the suite and derives a
   per-run worker prefix (`TC001-<run8>`), so cases and runs never
   contaminate each other. One advisory lock per environment
   (`tf-run:<env_id>`) serializes runs against the same database.
2. **Fixtures** — deterministic NDJSON generators produce the byte-exact
   files each case declares (worker ids are namespaced under the run prefix).
3. **Storage** — files are uploaded through the environment's storage backend
   (local dir or GCS).
4. **Ingest** — the runner calls your adapter's `ingest_file(...)` for each
   file (per-file mode) or once per merged batch (batch mode). The adapter
   must block until the load finishes.
5. **Assert** — the stored SQL expectations for each case are evaluated with
   `{placeholders}` bound from the run context (`prefix`, `file_id`,
   `as_of_*`, `target_worker`, ...). Four kinds: `sql_scalar`, `sql_row`,
   `file_bytes`, `file_rows`.
6. **Report** — a Markdown report is written and the full result tree is
   persisted in `tf.*` (`test_runs.summary` carries totals, ingest mode, and
   the batch plan).

## Batch ingest mode

`--ingest-mode per-file` (default) calls the adapter once per fixture file —
maximum isolation, one pipeline launch per file. `--ingest-mode batch` merges
batch-eligible cases' fixture files by `as_of_date` into one NDJSON per date
and calls the adapter once per group: the latest regression went from
**34 launches to 13**, all 23 cases green.

Eligibility is automatic: a case rides the batch unless it is flagged
`batchable=FALSE`, runs more than one execution, uses a poison generator
(malformed/invalid/empty fixtures), or its fixture files are byte-identical
(a replay test). Cases that stay solo ingest individually. See
`docs/ARCHITECTURE.md` and the batch contract in
`test_framework/batching.py`.

## Plug in your pipeline

1. Copy `examples/reference_adapter.py` into your codebase. It shows the full
   pattern: trigger your job (Dataflow, Spark, dbt, ...), **block until it
   completes**, return your file identity.
2. Register your environment row with
   `pipeline_mode='external'` and
   `ingest_adapter='mycompany.qa_adapter:ingest_file'`
   (see `docs/OPERATIONS.md`).
3. Author test cases against your own tables with
   `python3 -m test_framework.add_case --spec fixtures/samples/your-case.json`
   (dry-run validation first, `--apply` to upsert).

Assertion SQL is yours too: each case's `expectations` is arbitrary SQL
against your database. The 23 seeded cases target the reference schema;
keep them if your pipeline writes those tables, otherwise write your own.
The four assertion kinds are pipeline-agnostic except `file_rows`, which
verifies the framework's bronze contract (sha256-of-raw-line row identity,
`Worker_ID` natural key — see `test_framework/lineparse.py`).

## Deployment

See `docs/OPERATIONS.md` — the framework needs only PostgreSQL, a fixture
bucket, and your adapter. It runs on a laptop, a VM, or a Cloud Run Job
(`deploy/runner/`), with optional Composer scheduling (`deploy/composer/`).
