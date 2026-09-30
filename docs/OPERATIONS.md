# Operations

## Local development

Prerequisites: Python 3.12+, PostgreSQL 16, `psql`, `pytest`, `psycopg`.

```bash
export HR_PG_DSN="postgresql://hatch:hatch@127.0.0.1/hrdemo"
bash scripts/migrate.sh          # migrations + QA seed metadata (idempotent)
python3 -m pytest tests/unit -q
python3 -m test_framework.runner --env local --suite regression
```

## GCP deployment

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
