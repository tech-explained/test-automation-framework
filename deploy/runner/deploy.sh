#!/usr/bin/env bash
# Deploy the HR QA test framework as a Cloud Run Job and run the smoke suite.
#
# Prerequisites: scripts/gcp_bootstrap.sh has been run, migrations applied,
# and the gcp environment row registered (see docs/OPERATIONS.md).
#
# Usage: ./deploy/runner/deploy.sh [--suite smoke|regression] [--skip-build]
set -euo pipefail

SUITE="smoke"
SKIP_BUILD=0
while [ $# -gt 0 ]; do
  case "$1" in
    --suite) SUITE="$2"; shift 2 ;;
    --suite=*) SUITE="${1#*=}"; shift ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

PROJECT="$(gcloud config get-value project 2>/dev/null)"
: "${PROJECT:?set a gcloud project first: gcloud config set project <id>}"
REGION="${REGION:-us-central1}"
JOB="hr-qa-runner"
SA_EMAIL="hr-qa-runner@${PROJECT}.iam.gserviceaccount.com"
IMAGE="gcr.io/${PROJECT}/hr-qa-runner:latest"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "== project: $PROJECT  region: $REGION  suite: $SUITE"

if [ "$SKIP_BUILD" -eq 0 ]; then
  echo "== building runner image via Cloud Build"
  (cd "$ROOT" && gcloud builds submit --config deploy/runner/cloudbuild.yaml . \
    --project="$PROJECT" -q)
else
  echo "== skipping image build"
fi

echo "== rendering job manifest"
MANIFEST="$(mktemp)"
sed -e "s/__PROJECT__/${PROJECT}/g" -e "s/__REGION__/${REGION}/g" \
  "$ROOT/deploy/runner/job.yaml" > "$MANIFEST"

echo "== creating/updating Cloud Run Job $JOB"
if gcloud run jobs describe "$JOB" --region="$REGION" --project="$PROJECT" >/dev/null 2>&1; then
  gcloud run jobs replace "$MANIFEST" --region="$REGION" --project="$PROJECT" -q
else
  gcloud run jobs create "$JOB" --region="$REGION" --project="$PROJECT" \
    --image="$IMAGE" --service-account="$SA_EMAIL" \
    --vpc-connector="projects/${PROJECT}/locations/${REGION}/connectors/hr-qa-connector" \
    --set-env-vars="DATAFLOW_PG_DSN_SECRET=hr-postgres-dsn" \
    --set-secrets="HR_PG_DSN=hr-runner-dsn:latest" \
    --memory=2Gi --cpu=2 -q
fi
# A full regression launches one Dataflow job per case; give it room.
gcloud run jobs update "$JOB" --region="$REGION" --project="$PROJECT" \
  --task-timeout=7200 --max-retries=0 -q
rm -f "$MANIFEST"

echo "== executing: --env gcp --suite $SUITE"
gcloud run jobs execute "$JOB" --region="$REGION" --project="$PROJECT" \
  --wait --args="--env,gcp,--suite,${SUITE}"

echo
echo "done. Logs:"
echo "  gcloud logging read 'resource.type=\"cloud_run_job\" AND resource.labels.job_name=\"$JOB\"' \\"
echo "    --project=$PROJECT --limit=50 --format='value(textPayload)'"
echo "Run another suite:"
echo "  gcloud run jobs execute $JOB --region=$REGION --project=$PROJECT --wait --args='--env,gcp,--suite,regression'"
