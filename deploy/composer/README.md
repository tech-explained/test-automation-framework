# Triggering the QA framework from Cloud Composer

`dags/hr_qa_tests.py` runs the framework's suites inside GCP by executing
the `hr-qa-runner` Cloud Run Job and waiting for it to finish.

## Setup

1. Deploy the runner first: `deploy/runner/deploy.sh` (the job must exist).
2. Set your project in `dags/hr_qa_tests.py` (`PROJECT = ...`).
3. Upload the DAG to your Composer environment:

```bash
gcloud composer environments storage dags import \
  --environment <env-name> --location <region> \
  --source deploy/composer/dags/hr_qa_tests.py
```

4. Grant the Composer environment's service account permission to execute
   the job (the job itself still runs as `hr-qa-runner`):

```bash
COMPOSER_SA="$(gcloud composer environments describe <env-name> \
  --location <region> --format='value(config.nodeConfig.serviceAccount)')"
gcloud run jobs add-iam-policy-binding hr-qa-runner \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/run.developer" --region=us-central1
```

Requires `apache-airflow-providers-google >= 10.13.0` in the Composer
environment (for `container_overrides`).

## On demand

- Airflow UI: DAGs → `hr_qa_framework` → Trigger DAG (play icon). Both tasks
  run: `qa_smoke`, then `qa_regression` if smoke is green.
- CLI: `gcloud composer environments run <env-name> --location <region> dags trigger -- hr_qa_framework`
- Without Composer at all:
  `gcloud run jobs execute hr-qa-runner --region=us-central1 --wait --args='--env,gcp,--suite,smoke'`

## On schedule

Set `SCHEDULE` in the DAG to a cron expression, e.g. `"0 13 * * 1"`
(weekly, Monday 13:00 UTC), and re-upload. A full regression launches one
Dataflow job per test case, so prefer weekly over daily unless the budget
says otherwise. Task logs link back to the Cloud Run execution; the test
report itself is rendered to `test_framework/reports/` inside the job's
logs and persisted in `tf.test_runs` / `tf.test_case_results` /
`tf.assertion_results` in Cloud SQL.
