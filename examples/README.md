# Examples

These are **not part of the test framework**. They exist so you can run the
framework end-to-end locally and see exactly how to plug in your own pipeline.

## `reference_pipeline/`

A minimal HR data pipeline (Workday RaaS NDJSON → PostgreSQL, bronze/silver/gold,
SCD Type 4, content-addressed idempotency). It was the original bundled pipeline
the framework was built against; it now lives here as a working example.

- `launcher.ingest_file(...)` — the entry point the example adapter calls.
- `core.py` — pure logic (hashing, parsing, SCD decisions).
- `transforms.py` / `main.py` — Beam/Dataflow bronze loading.
- `dbio.py` — Postgres I/O.

## `reference_pipeline/db/`

The example's own database: `migrations/` (its `bronze`/`silver`/`gold`/
`ops` schema — not the framework's), `seeds/` (environments, suites, and
the 23-case HR QA matrix), and `apply.sh` which loads all of it plus the
three JSON-spec cases from `fixtures/samples/`. Run after the framework
migrations:

```bash
bash scripts/migrate.sh                          # tf.* tables (framework)
bash examples/reference_pipeline/db/apply.sh     # example schema + seeds
```

## `reference_adapter.py`

Implements the framework's ingest contract against the reference pipeline:

```python
def ingest_file(uri, *, file_name, as_of_date, env, actor) -> dict:
    ...
    return {"file_id": ..., "status": "completed", "error": None}
```

Copy this file as the starting point for your own adapter: trigger your
Dataflow/Spark/dbt job, **block until it finishes**, then return your
pipeline's file identity. Set `tf.environments.ingest_adapter` to your
module's dotted path (e.g. `mycompany.qa_adapter:ingest_file`).

The `local` environment seeds `ingest_adapter='examples.reference_adapter:ingest_file'`
so a fresh checkout runs the full regression with no GCP needed.
