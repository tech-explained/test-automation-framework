#!/usr/bin/env bash
# Applies migrations then seeds, in order. Requires HR_PG_DSN.
set -euo pipefail
: "${HR_PG_DSN:?HR_PG_DSN must be set}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
for f in "$ROOT"/db/migrations/*.sql; do
  echo "== $f"
  psql "$HR_PG_DSN" -v ON_ERROR_STOP=1 -q -f "$f"
done
for f in "$ROOT"/db/seeds/*.sql; do
  echo "== $f"
  psql "$HR_PG_DSN" -v ON_ERROR_STOP=1 -q -f "$f"
done
echo "migrations + seeds applied"
