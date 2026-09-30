# Runner image for the HR QA test framework.
# Build from the repo root: docker build -f deploy/runner/Dockerfile .
# (deploy/runner/deploy.sh does this via Cloud Build.)
#
# The image contains ONLY the framework + reference pipeline + DB/test assets.
# The pipeline/ package is included because the built-in local/dataflow
# ingest modes import pipeline.launcher lazily; pure framework-only runs
# with an external adapter never touch it.

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY test_framework/ ./test_framework/
COPY pipeline/ ./pipeline/
COPY db/ ./db/
COPY fixtures/ ./fixtures/
COPY scripts/ ./scripts/
COPY tests/ ./tests/

RUN pip install --no-cache-dir "psycopg[binary]" google-cloud-storage google-cloud-dataflow

# Cloud Run Job passes the real args at execution time, e.g.
#   --args="--env,gcp,--suite,smoke"
ENTRYPOINT ["python3", "-m", "test_framework.runner"]
CMD ["--help"]
