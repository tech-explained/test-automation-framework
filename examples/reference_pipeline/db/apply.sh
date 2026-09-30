#!/usr/bin/env bash
# Applies the reference pipeline's database: its own schema (bronze/silver/
# gold/ops), its seed data (environments, suites, 23-case QA matrix), and
# imports the three JSON-spec cases (TC-021..TC-023).
# Requires HR_PG_DSN. Run scripts/migrate.sh (framework tf.* tables) first.
set -euo pipefail
: "${HR_PG_DSN:?HR_PG_DSN must be set}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
shopt -s nullglob
for f in "$ROOT"/examples/reference_pipeline/db/migrations/*.sql; do
  echo "== $f"
  psql "$HR_PG_DSN" -v ON_ERROR_STOP=1 -q -f "$f"
done
for f in "$ROOT"/examples/reference_pipeline/db/seeds/*.sql; do
  echo "== $f"
  psql "$HR_PG_DSN" -v ON_ERROR_STOP=1 -q -f "$f"
done
for spec in "$ROOT"/fixtures/samples/tc-*.json; do
  echo "== add_case $spec"
  python3 -m test_framework.add_case --spec "$spec" --apply
done
echo "reference pipeline database applied"
