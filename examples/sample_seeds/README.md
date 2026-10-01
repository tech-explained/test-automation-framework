# Sample seeds

These SQL files are **examples**, not framework code. They show the shape
of `tf.*` metadata: environments, pipelines, suites, and test cases.

- `001_sample_seeds.sql` — sample environments, a sample pipeline row,
  and example test cases TC-001..TC-008.
- `002_sample_seeds.sql` — example test cases TC-009..TC-020 plus suites.

The test-case SQL references illustrative table names (`bronze.*`,
`silver.*`, `ops.*`). **Adapt every assertion to your own pipeline's
tables** — the framework doesn't care what your schema looks like, only
that your assertions' SQL is valid against it.

To use these as a starting point:

1. Copy them aside and rewrite the test-case SQL for your tables.
2. Set `ingest_adapter` in `tf.environments` to your adapter's dotted
   path (see `examples/adapter_template.py`).
3. Apply with `psql "$HR_PG_DSN" -f <file>` after `scripts/migrate.sh`.

For programmatic case authoring, see `test_framework/add_case.py` and
the JSON specs in `fixtures/samples/`.
