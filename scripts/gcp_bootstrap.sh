#!/usr/bin/env bash
# One-time GCP bootstrap for running the HR QA test framework inside GCP.
#
# Creates (idempotently — safe to re-run):
#   * required APIs
#   * service account hr-qa-runner + IAM (Dataflow developer, Cloud SQL client,
#     fixture-bucket objectAdmin, secretAccessor on the two DSN secrets)
#   * Cloud SQL for PostgreSQL instance + database + user
#   * GCS fixture bucket
#   * Secret Manager secrets: hr-postgres-dsn (workers), hr-runner-dsn (runner)
#   * Serverless VPC Access connector (Cloud Run Job -> Cloud SQL private IP)
#
# After this: run scripts/migrate.sh with HR_PG_DSN from the runner secret,
# register the gcp environment row (see docs/OPERATIONS.md), then
# deploy/runner/deploy.sh.
#
# Usage: ./scripts/gcp_bootstrap.sh   (uses current gcloud project)

set -euo pipefail

PROJECT="$(gcloud config get-value project 2>/dev/null)"
: "${PROJECT:?set a gcloud project first: gcloud config set project <id>}"

REGION="${REGION:-us-central1}"
SQL_INSTANCE="${SQL_INSTANCE:-hr-qa-db}"
DB_NAME="${DB_NAME:-hrprod}"
DB_USER="${DB_USER:-qa}"
BUCKET="${BUCKET:-${PROJECT}-hr-qa-fixtures}"
SA_NAME="${SA_NAME:-hr-qa-runner}"
SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
SECRET_WORKER="${SECRET_WORKER:-hr-postgres-dsn}"
SECRET_RUNNER="${SECRET_RUNNER:-hr-runner-dsn}"
VPC_CONNECTOR="${VPC_CONNECTOR:-hr-qa-connector}"
NETWORK="${NETWORK:-default}"

have() { gcloud "$@" >/dev/null 2>&1; }

echo "== project: $PROJECT  region: $REGION"

echo "== enabling APIs"
gcloud services enable \
  dataflow.googleapis.com sqladmin.googleapis.com storage.googleapis.com \
  secretmanager.googleapis.com run.googleapis.com cloudbuild.googleapis.com \
  vpcaccess.googleapis.com --project="$PROJECT" -q

echo "== service account $SA_EMAIL"
if ! have iam service-accounts describe "$SA_EMAIL" --project="$PROJECT"; then
  gcloud iam service-accounts create "$SA_NAME" \
    --display-name="HR QA test framework runner" --project="$PROJECT" -q
fi
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${SA_EMAIL}" --role="roles/dataflow.developer" -q >/dev/null
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${SA_EMAIL}" --role="roles/cloudsql.client" -q >/dev/null

echo "== Cloud SQL instance $SQL_INSTANCE"
if ! have sql instances describe "$SQL_INSTANCE" --project="$PROJECT"; then
  gcloud sql instances create "$SQL_INSTANCE" \
    --database-version=POSTGRES_15 --cpu=2 --memory=7680MB \
    --region="$REGION" --network="projects/${PROJECT}/global/networks/${NETWORK}" \
    --no-assign-ip --project="$PROJECT" -q
fi
if ! have sql databases describe "$DB_NAME" --instance="$SQL_INSTANCE" --project="$PROJECT"; then
  gcloud sql databases create "$DB_NAME" --instance="$SQL_INSTANCE" --project="$PROJECT" -q
fi

PRIVATE_IP="$(gcloud sql instances describe "$SQL_INSTANCE" --project="$PROJECT" \
  --format='value(ipAddresses[0].ipAddress)')"

DB_PASSWORD="$(openssl rand -base64 18 | tr -d '/+=' | head -c 20)"
if gcloud sql users describe "$DB_USER" --instance="$SQL_INSTANCE" --project="$PROJECT" >/dev/null 2>&1; then
  gcloud sql users set-password "$DB_USER" --instance="$SQL_INSTANCE" \
    --password="$DB_PASSWORD" --project="$PROJECT" -q
else
  gcloud sql users create "$DB_USER" --instance="$SQL_INSTANCE" \
    --password="$DB_PASSWORD" --project="$PROJECT" -q
fi

echo "== GCS bucket gs://$BUCKET"
if ! have storage buckets describe "gs://$BUCKET" --project="$PROJECT"; then
  gcloud storage buckets create "gs://$BUCKET" --location="$REGION" --project="$PROJECT"
fi
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:${SA_EMAIL}" --role="roles/storage.objectAdmin" \
  --project="$PROJECT" -q >/dev/null

DSN="postgresql://${DB_USER}:${DB_PASSWORD}@${PRIVATE_IP}:5432/${DB_NAME}"
echo "== secrets $SECRET_WORKER / $SECRET_RUNNER"
for s in "$SECRET_WORKER" "$SECRET_RUNNER"; do
  if ! have secrets describe "$s" --project="$PROJECT"; then
    printf '%s' "$DSN" | gcloud secrets create "$s" --data-file=- \
      --replication-policy=automatic --project="$PROJECT" -q
  else
    printf '%s' "$DSN" | gcloud secrets versions add "$s" --data-file=- \
      --project="$PROJECT" -q
  fi
  gcloud secrets add-iam-policy-binding "$s" --project="$PROJECT" \
    --member="serviceAccount:${SA_EMAIL}" --role="roles/secretmanager.secretAccessor" -q >/dev/null
done

echo "== VPC connector $VPC_CONNECTOR"
if ! have compute networks vpc-access connectors describe "$VPC_CONNECTOR" \
    --region="$REGION" --project="$PROJECT"; then
  gcloud compute networks vpc-access connectors create "$VPC_CONNECTOR" \
    --region="$REGION" --network="$NETWORK" --range=10.8.0.0/28 --project="$PROJECT" -q
fi

echo
echo "bootstrap complete. Next steps:"
echo "  1. export HR_PG_DSN=\"\$(gcloud secrets versions access latest --secret=$SECRET_RUNNER --project=$PROJECT)\""
echo "     # needs private-IP reachability: run from a VM in $NETWORK, or Cloud Shell with private-services access"
echo "  2. bash scripts/migrate.sh   # tf.* schema (framework only)"
echo "  3. register the gcp environment row (docs/OPERATIONS.md)"
echo "  4. deploy/runner/deploy.sh   # build image, create Cloud Run Job, run smoke"
echo
echo "Note: the DB password was rotated and stored only in Secret Manager."
