"""HR QA test framework — on-demand and scheduled runs via Cloud Run Job.

Runs the framework's suites inside GCP by executing the hr-qa-runner
Cloud Run Job (built by deploy/runner/deploy.sh) and waiting for it.

ON DEMAND (default):
  * Airflow UI -> DAGs -> hr_qa_framework -> Trigger DAG (play icon), or
  * gcloud composer environments run <env> --location <region> \
        dags trigger -- hr_qa_framework
  * plain gcloud (no Composer needed):
    gcloud run jobs execute hr-qa-runner --region=us-central1 --wait \
      --args='--env,gcp,--suite,smoke'

SCHEDULED:
  Set SCHEDULE below to a cron expression, e.g. "0 13 * * 1"
  (Mondays 13:00 UTC). Note each regression run launches one Dataflow job
  per test case — schedule with cost in mind.

Requires apache-airflow-providers-google >= 10.13.0 (container_overrides).
The Composer environment's service account needs roles/run.developer on the
hr-qa-runner job (see deploy/composer/README.md).
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)
from airflow.utils.dates import days_ago

PROJECT = "REPLACE_WITH_GCP_PROJECT"
REGION = "us-central1"
JOB_NAME = "hr-qa-runner"

# None = on-demand only. Cron examples: "@daily", "0 13 * * 1".
SCHEDULE = None

default_args = {
    "owner": "qa",
    # No retries: a retry would record a second test run; the report must
    # reflect the single actual run.
    "retries": 0,
}


def _qa_task(task_id, suite):
    """Execute the Cloud Run Job with the suite as container args; wait."""
    return CloudRunExecuteJobOperator(
        task_id=task_id,
        project_id=PROJECT,
        region=REGION,
        job_name=JOB_NAME,
        overrides={
            "container_overrides": [{"args": ["--env", "gcp", "--suite", suite]}]
        },
        # A full regression launches one Dataflow job per case; allow 2h.
        timeout_seconds=2 * 60 * 60,
    )


with DAG(
    dag_id="hr_qa_framework",
    description="Run the HR QA test framework suites inside GCP",
    schedule=SCHEDULE,
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["qa", "hr"],
) as dag:
    smoke = _qa_task("qa_smoke", "smoke")
    regression = _qa_task("qa_regression", "regression")

    # Regression only runs if smoke is green.
    smoke >> regression
