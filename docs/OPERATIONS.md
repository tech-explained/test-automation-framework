# Operations

## Local development

Prerequisites: Python 3.12+, PostgreSQL 16, `psql`, `pytest`, `psycopg`.

```bash
export HR_PG_DSN="postgresql://hatch:hatch@127.0.0.1/hrdemo"
bash scripts/migrate.sh          # migrations + QA seed metadata (idempotent)
python3 -m pytest tests/unit -q
python3 -m test_framework.runner --env local --suite regression
```

## Framework-only deployment (pipeline developed separately)

The test framework is standalone: it only needs PostgreSQL, a fixture
bucket, and one adapter function. The `pipeline/` package is NOT required
(the runner imports it lazily, only for the built-in `local`/`dataflow`
modes).

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

Assertion SQL is yours too: each test case's `expectations` is arbitrary SQL
against your database. The 23 seeded cases target the reference schema
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
`gold.*` migrations are the reference pipeline's schema — apply them only if
your pipeline adopts that contract.

### 2. Fixture bucket

One GCS bucket (or prefix) the runner can write fixtures to. The runner's
identity needs `roles/storage.objectAdmin` on it (or objectCreator +
objectViewer).

### 3. Runner host

Anywhere with Python 3.12+, `psycopg`, DB access and GCS access: a GCE VM,
a Cloud Run Job, Cloud Build, or a laptop. No Docker, no Dataflow.

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

### 5. Run

```bash
export HR_PG_DSN="postgresql://user:pass@10.x.x.x/hrprod"
python3 -m test_framework.runner --env gcp --suite smoke
python3 -m test_framework.runner --env gcp --suite regression
```

IAM summary (framework-only): runner identity needs Cloud SQL client,
Secret Manager accessor only if you keep the DSN in Secret Manager, and
GCS objectAdmin on the fixture bucket. No Dataflow roles, no worker service
account.

## Deploy and run the framework inside GCP (Cloud Run Job)

This runs the framework itself on GCP — no laptop needed. The runner executes
as a Cloud Run Job in your project, talking to real GCS, Dataflow, and Cloud
SQL over the VPC.

```bash
gcloud config set project <project>

# 1. one-time infra: APIs, service account + IAM, Cloud SQL, GCS bucket,
#    Secret Manager DSNs, VPC connector  (idempotent, safe to re-run)
./scripts/gcp_bootstrap.sh

# 2. apply the framework schema (needs private-IP reachability to Cloud SQL,
#    e.g. a VM on the same VPC or Cloud Shell with private-services access)
export HR_PG_DSN="$(gcloud secrets versions access latest --secret=hr-runner-dsn)"
bash scripts/migrate.sh

# 3. register the gcp environment row + template path (SQL in the
#    "GCP deployment (bundled reference pipeline)" section below)

# 4. build the runner image, create the Cloud Run Job, run the smoke suite
./deploy/runner/deploy.sh
```

What `deploy.sh` does: builds `gcr.io/<project>/hr-qa-runner:latest` via Cloud
Build (`deploy/runner/Dockerfile` + `cloudbuild.yaml`), creates/updates the
`hr-qa-runner` job from `deploy/runner/job.yaml` (service account, VPC
connector for Cloud SQL private IP, `HR_PG_DSN` from Secret Manager), sets a
2-hour task timeout, then executes `--env gcp --suite smoke` and waits.

Run other suites afterwards:

```bash
gcloud run jobs execute hr-qa-runner --region=us-central1 --wait \
  --args='--env,gcp,--suite,regression'
gcloud run jobs execute hr-qa-runner --region=us-central1 --wait \
  --args='--env,gcp,--cases,TC-001,TC-005'
```

Files: `scripts/gcp_bootstrap.sh`, `deploy/runner/{Dockerfile,cloudbuild.yaml,job.yaml,deploy.sh}`.
Notes: each regression case launches its own Dataflow job (cost); the VPC
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

## GCP deployment (bundled reference pipeline)

### 1. Network + database

- Cloud SQL for PostgreSQL 15+ with **private IP** on a dedicated VPC.
- Apply `db/migrations/*.sql` from a host with VPC access (Cloud Shell with
  private-services access, or a GCE jump host). Seeds `008_*` are optional in
  prod — they seed the QA metadata, harmless to include.
- Store the DSN in Secret Manager, e.g. `hr-postgres-dsn`
  (`postgresql://user:pass@10.x.x.x/hrprod`).

### 2. IAM (least privilege)

- Dataflow worker service account: `roles/dataflow.worker`,
  `roles/storage.objectViewer` (input bucket) + `objectCreator` (template
  staging), `roles/secretmanager.secretAccessor` (only the DSN secret),
  `roles/cloudsql.client` (or authorized-network IP).
- Launcher identity (whoever runs `ingest_file` in dataflow mode):
  `roles/dataflow.developer` on the project.

### 3. Build + deploy the Flex Template

```bash
cd pipeline
docker build -t gcr.io/<project>/hr-workday-loader:latest .
docker push gcr.io/<project>/hr-workday-loader:latest
gcloud dataflow flex-template build \
  gs://<bucket>/templates/hr-workday-ndjson.json \
  --image gcr.io/<project>/hr-workday-loader:latest \
  --sdk-language PYTHON \
  --metadata-file metadata.json
```

### 4. Register the gcp environment

```sql
UPDATE tf.environments SET active = TRUE WHERE env_id = 'gcp';
-- set gcs_bucket, gcp_project, dataflow_region, pipeline_mode='dataflow'
UPDATE tf.pipelines
   SET flex_template_gcs_path = 'gs://<bucket>/templates/hr-workday-ndjson.json'
 WHERE pipeline_id = 'hr-workday-ndjson-v1';
```

### 5. Run the QA suite against GCP

```bash
export HR_PG_DSN="<cloud sql dsn>"            # framework's own DB handle
export DATAFLOW_PG_DSN_SECRET="hr-postgres-dsn"
python3 -m test_framework.runner --env gcp --suite smoke
python3 -m test_framework.runner --env gcp --suite regression
```

### 6. Production loads

```python
from datetime import date
from pipeline import launcher

launcher.ingest_file(
    "gs://hr-extracts/workday/workers_20260929.ndjson",
    as_of_date=date(2026, 9, 29),
    mode="dataflow",
    actor="scheduler",
    dataflow={
        "project": "<project>",
        "region": "us-central1",
        "template_gcs_path": "gs://<bucket>/templates/hr-workday-ndjson.json",
        "pg_dsn_secret": "hr-postgres-dsn",
    },
)
```

## Monitoring

- `ops.file_ingestions`: watch `status`, `rows_rejected`, `dq_warnings`.
- `ops.audit_log`: the full transition trail per `file_id`.
- Dataflow console: job graph, failed bundles → check `bronze.raw_worker_rejects`
  for the quarantined lines.
- Gold views power BI directly; `gold.vw_missing_from_latest_snapshot`
  flags workers that vanished from the newest extract.

## Incident notes

- **File stuck in `processing`**: a previous launcher died mid-run. Inspect,
  then either re-run (bronze writes are idempotent) or manually reset status.
- **Replay storms**: byte-identical files are free (`skipped_duplicate`);
  reordered-but-identical business data costs one bronze write and zero new
  silver versions.
- **Schema drift**: new RaaS fields appear in `attributes` automatically;
  promote to typed columns via migration when analysts need them.
