#!/usr/bin/env bash
# Applies the framework migrations (tf.* tables only). Requires HR_PG_DSN.
# The framework ships with no seed data; sample seeds live with the
# sample seeds (examples/sample_seeds/).
set -euo pipefail
: "${HR_PG_DSN:?HR_PG_DSN must be set}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
shopt -s nullglob
for f in "$ROOT"/db/migrations/*.sql; do
  echo "== $f"
  psql "$HR_PG_DSN" -v ON_ERROR_STOP=1 -q -f "$f"
done
for f in "$ROOT"/db/seeds/*.sql; do
  echo "== $f"
  psql "$HR_PG_DSN" -v ON_ERROR_STOP=1 -q -f "$f"
done
echo "framework migrations applied"
