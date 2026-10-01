# Examples

These are **not part of the test framework**. They are starting points for
plugging your own pipeline in and for authoring test cases.

## `adapter_template.py`

The ingest contract, documented with a Dataflow Flex Template skeleton.
Copy it into your codebase, implement `ingest_file()` for your pipeline,
and set `tf.environments.ingest_adapter` to your module's dotted path
(e.g. `mycompany.qa_adapter:ingest_file`). The framework calls this one
function per fixture file and blocks on it — trigger your job, **wait
until it finishes**, return your pipeline's file identity.

## `sample_seeds/`

Example `tf.*` seed data: sample environments, a sample pipeline row,
suites, and illustrative test cases (`001_sample_seeds.sql`,
`002_sample_seeds.sql`). The case SQL references illustrative table names
— **adapt every assertion to your own pipeline's tables** before using.
Apply after the framework migrations:

```bash
bash scripts/migrate.sh
psql "$HR_PG_DSN" -f examples/sample_seeds/001_sample_seeds.sql
psql "$HR_PG_DSN" -f examples/sample_seeds/002_sample_seeds.sql
```

For programmatic case authoring, prefer `test_framework/add_case.py` with
JSON specs (see `fixtures/samples/`).
