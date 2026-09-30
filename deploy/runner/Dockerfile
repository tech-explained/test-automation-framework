# Runner image for the HR QA test framework.
# Build from the repo root: docker build -f deploy/runner/Dockerfile .
# (deploy/runner/deploy.sh does this via Cloud Build.)
#
# The image contains ONLY the framework + DB/test assets. Your pipeline's
# adapter is baked in by copying your adapter package and setting
# tf.environments.ingest_adapter to its dotted path. The examples/
# reference pipeline is included so the seeded 'local' environment works
# out of the box; pure external-adapter runs never touch it.

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY test_framework/ ./test_framework/
COPY examples/ ./examples/
COPY db/ ./db/
COPY fixtures/ ./fixtures/
COPY scripts/ ./scripts/
COPY tests/ ./tests/

RUN pip install --no-cache-dir "psycopg[binary]" google-cloud-storage

# Cloud Run Job passes the real args at execution time, e.g.
#   --args="--env,gcp,--suite,smoke"
ENTRYPOINT ["python3", "-m", "test_framework.runner"]
CMD ["--help"]
