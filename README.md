# Test Automation Framework

Metadata-driven QA for **any** data pipeline. You give the framework one
function — *load this file through my pipeline* — and it generates
deterministic fixtures, loads them through your pipeline, and evaluates
stored SQL assertions against your database. No YAML, no config drift: test
cases, environments, suites, and every result live in PostgreSQL (`tf.*`).

![Architecture diagram](docs/architecture-diagram.png)
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
(`module.path:function_name`). A commented template is at
`examples/adapter_template.py`.

## Quickstart

```bash
export HR_PG_DSN="postgresql://user:pass@host/dbname"

# 1. apply the framework migrations (tf.* tables only; idempotent)
bash scripts/migrate.sh

# 2. unit tests
python3 -m pytest tests/unit -q

# 3. register your environment (see docs/OPERATIONS.md) and run
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
| `db/seeds/` | Empty by design — the framework ships no seed data. Sample seeds live in `examples/sample_seeds/`. |
| `examples/` | **Not the framework.** `adapter_template.py` (the ingest contract template), `sample_seeds/` (example environments, suites, and test cases — adapt to your tables). |
| `tests/unit/` | pytest suite for the framework: fixtures, assertions, batching, adapter. |
| `fixtures/samples/` | JSON test-case specs consumed by `test_framework/add_case.py`. |
| `deploy/runner/` | Cloud Run Job packaging for running the framework itself on GCP. |
| `deploy/composer/` | Cloud Composer DAG that triggers the runner job. |
| `docs/` | `HLD.md` (high-level design), `LLD.md` (low-level design), `ARCHITECTURE.md`, `TEST_PLAN.md`, `OPERATIONS.md`, `architecture-diagram.svg/png`, `schema-diagram.svg/png`. |

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
and calls the adapter once per group, cutting pipeline launches sharply.

Eligibility is automatic: a case rides the batch unless it is flagged
`batchable=FALSE`, runs more than one execution, uses a poison generator
(malformed/invalid/empty fixtures), or its fixture files are byte-identical
(a replay test). Cases that stay solo ingest individually. See
`docs/ARCHITECTURE.md` and the batch contract in
`test_framework/batching.py`.

## Plug in your pipeline

1. Copy `examples/adapter_template.py` into your codebase. It documents the
   full contract: trigger your job (Dataflow, Spark, dbt, ...), **block
   until it completes**, return your file identity.
2. Register your environment row with
   `pipeline_mode='external'` and
   `ingest_adapter='mycompany.qa_adapter:ingest_file'`
   (see `docs/OPERATIONS.md`).
3. Author test cases against your own tables with
   `python3 -m test_framework.add_case --spec fixtures/samples/your-case.json`
   (dry-run validation first, `--apply` to upsert). Sample cases in
   `examples/sample_seeds/` show the shape — rewrite the SQL for your
   tables.

Assertion SQL is yours too: each case's `expectations` is arbitrary SQL
against your database. The four assertion kinds are pipeline-agnostic except
`file_rows`, which verifies the framework's bronze contract (sha256-of-raw-line
row identity, `Worker_ID` natural key — see `test_framework/lineparse.py`).

## Deployment

See `docs/OPERATIONS.md` — the framework needs only PostgreSQL, a fixture
bucket, and your adapter. It runs on a laptop, a VM, or a Cloud Run Job
(`deploy/runner/`), with optional Composer scheduling (`deploy/composer/`).
