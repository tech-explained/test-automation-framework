# Operations

## Local development

Prerequisites: Python 3.12+, PostgreSQL 16, `psql`, `pytest`, `psycopg`.

```bash
export HR_PG_DSN="postgresql://hatch:hatch@127.0.0.1/hrdemo"
bash scripts/migrate.sh          # migrations + QA seed metadata (idempotent)
python3 -m pytest tests/unit -q
python3 -m test_framework.runner --env local --suite regression
```

The seeded `local` environment runs the framework against the example
adapter (`examples.reference_adapter:ingest_file`), so everything works
offline with no GCP.

## Framework-only deployment (pipeline developed separately)

The test framework is standalone: it needs PostgreSQL, a fixture bucket,
and one adapter function. It never imports the pipeline under test.

### What the framework needs from your pipeline

Exactly one seam — implement this function anywhere importable:

```python
def ingest_file(uri, *, file_name, as_of_date, env, actor) -> dict:
    """Load the fixture file at `uri` through the pipeline under test.

    MUST block until the load is finished (the runner evaluates assertions
    immediately after this returns).
    Returns {"file_id": str, "status": "completed"|"failed"|"skipped_duplicate",
             "error": str | None}.
    `file_id` feeds the {file_id} assertion placeholder: use whatever file
    identity your pipeline has (registry id, load id, ...).
    """
```

Start from `examples/reference_adapter.py` — it implements this contract
against the example pipeline (trigger → block until done → return identity).

Assertion SQL is yours too: each test case's `expectations` is arbitrary SQL
against your database. The 23 seeded cases target the example schema
(`bronze.*`, `silver.*`, `ops.*`); keep them if your pipeline writes those
tables, otherwise author cases with `add_case.py` against your own tables.
The four assertion kinds (`sql_scalar`, `sql_row`, `file_bytes`,
`file_rows`) are pipeline-agnostic except `file_rows`, which verifies the
framework's bronze contract (sha256-of-raw-line row identity, `Worker_ID`
natural key — see `test_framework/lineparse.py`).

### 1. Database

Cloud SQL for PostgreSQL 15+ (private IP on a dedicated VPC is enough; no
Dataflow/Secret Manager needed for framework-only). From a host with VPC
access:

```bash
export HR_PG_DSN="postgresql://user:pass@10.x.x.x/hrprod"
bash scripts/migrate.sh   # tf.* schema + seed cases; idempotent
```

Only the `tf.*` tables are required. `bronze.*` / `silver.*` / `ops.*` /
`gold.*` migrations are the example pipeline's schema — apply them only if
your pipeline adopts that contract.

### 2. Fixture bucket

One GCS bucket (or prefix) the runner can write fixtures to. The runner's
identity needs `roles/storage.objectAdmin` on it (or objectCreator +
objectViewer).

### 3. Runner host

Anywhere with Python 3.12+, `psycopg`, DB access and GCS access: a GCE VM,
a Cloud Run Job, Cloud Build, or a laptop. No Docker, no pipeline binaries.

```bash
pip install "psycopg[binary]" google-cloud-storage
```

### 4. Register the environment

```sql
INSERT INTO tf.environments
    (env_id, display_name, pipeline_mode, storage_backend,
     gcp_project, gcs_bucket, db_dsn_env_var, ingest_adapter, active)
VALUES
    ('gcp', 'GCP (external pipeline + GCS fixtures)',
     'external', 'gcs',
     '<project>', '<bucket>', 'HR_PG_DSN',
     'mycompany.qa_adapter:ingest_file', TRUE);
```

`pipeline_mode` is always `'external'`; the framework resolves the adapter
from `ingest_adapter` at run time.

### 5. Run

```bash
export HR_PG_DSN="postgresql://user:pass@10.x.x.x/hrprod"
python3 -m test_framework.runner --env gcp --suite smoke
python3 -m test_framework.runner --env gcp --suite regression
python3 -m test_framework.runner --env gcp --suite regression --ingest-mode batch
```

IAM summary (framework-only): runner identity needs Cloud SQL client,
Secret Manager accessor only if you keep the DSN in Secret Manager, and
GCS objectAdmin on the fixture bucket. No pipeline-specific roles needed
by the framework — your adapter's own credentials cover triggering your
pipeline.

## Deploy and run the framework inside GCP (Cloud Run Job)

This runs the framework itself on GCP — no laptop needed. The runner
executes as a Cloud Run Job in your project, talking to GCS, Cloud SQL,
and your pipeline (via your adapter) over the VPC.

```bash
gcloud config set project <project>

# 1. one-time infra: APIs, service account + IAM, Cloud SQL, GCS bucket,
#    Secret Manager DSNs, VPC connector  (idempotent, safe to re-run)
./scripts/gcp_bootstrap.sh

# 2. apply the framework schema (needs private-IP reachability to Cloud SQL,
#    e.g. a VM on the same VPC or Cloud Shell with private-services access)
export HR_PG_DSN="$(gcloud secrets versions access latest --secret=hr-runner-dsn)"
bash scripts/migrate.sh

# 3. register the gcp environment row pointing at YOUR adapter
#    (pipeline_mode='external', ingest_adapter='mycompany.qa_adapter:ingest_file')
#    — see "4. Register the environment" above

# 4. build the runner image, create the Cloud Run Job, run the smoke suite
./deploy/runner/deploy.sh
```

What `deploy.sh` does: builds `gcr.io/<project>/hr-qa-runner:latest` via Cloud
Build (`deploy/runner/Dockerfile` + `cloudbuild.yaml`), creates/updates the
`hr-qa-runner` job from `deploy/runner/job.yaml` (service account, VPC
connector for Cloud SQL private IP, `HR_PG_DSN` from Secret Manager), sets a
2-hour task timeout, then executes `--env gcp --suite smoke` and waits.

The image contains only the framework + `examples/` + DB/test assets. Your
adapter is baked in by copying your adapter package into the image and
setting `tf.environments.ingest_adapter` to its dotted path (see the
Dockerfile comments).

Run other suites afterwards:

```bash
gcloud run jobs execute hr-qa-runner --region=us-central1 --wait \
  --args='--env,gcp,--suite,regression'
gcloud run jobs execute hr-qa-runner --region=us-central1 --wait \
  --args='--env,gcp,--suite,regression,--ingest-mode,batch'
gcloud run jobs execute hr-qa-runner --region=us-central1 --wait \
  --args='--env,gcp,--cases,TC-001,TC-005'
```

Files: `scripts/gcp_bootstrap.sh`, `deploy/runner/{Dockerfile,cloudbuild.yaml,job.yaml,deploy.sh}`.
Notes: in per-file mode each case triggers its own pipeline launch through
your adapter (cost scales with launches — batch mode cuts them); the VPC
connector is required because Cloud SQL uses a private IP; the DB password
lives only in Secret Manager (bootstrap rotates it).

### Triggering on demand or on schedule (Cloud Composer)

`deploy/composer/dags/hr_qa_tests.py` is a ready-to-upload DAG that executes
the `hr-qa-runner` Cloud Run Job via `CloudRunExecuteJobOperator` and waits
for it: `qa_smoke` → `qa_regression` (regression only if smoke is green).
`SCHEDULE` defaults to `None` (on-demand: trigger from the Airflow UI or
`dags trigger`); set it to a cron expression for scheduled runs. Setup
(DAG upload, IAM for the Composer service account) is in
`deploy/composer/README.md`.

## Monitoring

- `tf.test_runs` / `tf.case_results` / `tf.assertion_results`: every run's
  pass/fail tree. `test_runs.summary` carries case totals, `ingest_mode`,
  and the batch plan (groups, launches) for batch-mode runs.
- `test_framework/reports/<run_id>.md`: the human-readable report per run.
- Your adapter's own logs/metrics: pipeline launch latency, load statuses
  returned per `file_id`, and any pipeline-side errors surfaced as `error`
  in the adapter result.

## Incident notes

- **Adapter call never returns**: the adapter must block until the load
  finishes — a hang here hangs the run. Check your pipeline's job state
  directly (stalled job, quota, missing trigger permission) and kill the
  runner; the env advisory lock is released on exit, and the run can be
  re-executed.
- **Run fails to start on the env lock**: a previous runner died while
  holding the `tf-run:<env_id>` advisory lock. Verify no runner is alive,
  then release the lock (`SELECT pg_advisory_unlock_all()` on that session
  or restart cleanly) and re-run.
- **Replay storms**: byte-identical fixtures short-circuit as
  `skipped_duplicate` if your adapter implements that status; otherwise
  they cost a full load — cheap either way, and per-file mode isolates
  them by design.
- **`file_rows` mismatches**: the framework checks raw-line identity
  against the pipeline's stored rows (sha256 of the raw line, `Worker_ID`
  natural key). Mismatches usually mean the pipeline normalizes or rewrites
  lines on the way in — switch those assertions to `sql_scalar` against
  your own tables.
