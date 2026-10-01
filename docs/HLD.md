# High Level Design — Test Automation Framework

## 1. Purpose

A metadata-driven test framework for **any** data pipeline that loads files
and lands them in a queryable store (PostgreSQL today). The framework
generates deterministic test input files, loads them through the pipeline
under test, and evaluates stored SQL assertions against the results. All
test definitions and all results live in PostgreSQL — there are no YAML or
JSON config files to drift.

The framework is pipeline-agnostic by construction: it never imports,
embeds, or assumes anything about the pipeline's internals. The entire
integration is a single function the pipeline owner provides (the *ingest
adapter*).

## 2. Design goals

1. **One seam.** The pipeline plugs in through exactly one callable
   (`ingest_file`). Everything else is the framework's business.
2. **QA as data.** Test cases, environments, suites, runs, and results are
   rows in `tf.*` tables. A run is reproducible from the database alone;
   the Markdown report is a query, not a log scrape.
3. **Deterministic fixtures.** Every fixture file is a pure function of its
   inputs — byte-identical on every run — so replays, idempotency tests,
   and failure reproduction are meaningful.
4. **Isolation by construction.** Per-run worker namespaces and one advisory
   lock per environment mean cases and runs cannot contaminate each other,
   even when their data shares a single pipeline load.
5. **Cheap at scale.** Batch ingest mode merges eligible cases' files so one
   pipeline launch can serve many test cases, with per-case assertion
   safety preserved.
6. **Never a run-killer.** Assertion SQL errors, adapter failures, and
   malformed fixtures become recorded results (`error` / `fail`), never
   crashes that abort the run.

## 3. System context

```
                    ┌──────────────────────────────┐
                    │   Test Automation Framework  │
                    │                              │
  Test author ─────▶│  tf.* metadata (cases,       │
                    │  suites, envs)               │
                    │                              │
                    │  runner: fixtures → storage  │
                    │          → ingest → assert   │
                    │          → report            │
                    └──────────────┬───────────────┘
                                   │ ingest_file(uri, ...)
                                   │ (blocks until load finishes)
                                   ▼
                    ┌──────────────────────────────┐
                    │   Pipeline under test        │
                    │   (YOUR system: Dataflow,   │
                    │   Spark, dbt, ...)           │
                    │                              │
                    │   Your tables ◀── assertions │
                    │   read them (SQL)            │
                    └──────────────────────────────┘
```

The framework owns the left box: PostgreSQL `tf.*` tables, the runner, and
fixture generation. The pipeline under test owns the right box: its own
code, its own schema, its own tables. The arrow between them is the only
coupling — one function call per fixture file — and assertions are plain
SQL the test author writes against the pipeline's own tables.

## 4. Component overview

| Component | Responsibility |
|---|---|
| `runner` | Orchestrates a run: resolve cases → generate fixtures → upload → ingest via adapter → evaluate assertions → persist results → render report. |
| `pipeline_adapter` | Resolves `tf.environments.ingest_adapter` (dotted path) and invokes the pipeline's `ingest_file`. The only framework code that touches the pipeline. |
| `fixtures` | Deterministic NDJSON generators (registry `GENERATORS`). Pure functions of `(params, prefix)` → byte-identical files. |
| `storage` | Backend abstraction for fixture files: `local` (directory as bucket) or `gcs` (real Cloud Storage). |
| `assertions` | Evaluates the four assertion kinds (`sql_scalar`, `sql_row`, `file_bytes`, `file_rows`) with `{placeholders}` bound as query parameters. |
| `batching` | Plans batch ingest: eligibility rules + grouping fixture files by `as_of_date` into merged loads. |
| `reporting` | Renders the Markdown run report from `tf.*` rows. |
| `add_case` | Validates and upserts test-case JSON specs into `tf.test_cases`. |
| `lineparse` | Canonical raw-line identity (SHA-256 of the raw line, `Worker_ID` extraction) backing the `file_rows` assertion kind. |
| `db` | Migration runner + connection helpers for `tf.*`. |

Supporting assets: `db/migrations/` (the `tf.*` schema), `deploy/runner/`
(Cloud Run Job packaging), `deploy/composer/` (Composer DAG trigger),
`examples/` (adapter template + sample seeds — not framework code).

## 5. Key design decisions

### 5.1 Metadata, not configuration

Test cases are rows, not files. `tf.test_cases` holds the fixture sequence
(JSONB generator specs) and the expectations (JSONB assertion list);
`tf.suites` holds ordered case lists; `tf.environments` holds the adapter
pointer and storage config. This kills config drift: the thing that ran is
the thing stored, and re-running a historical case is a database read.

### 5.2 The external-only adapter seam

`tf.environments.pipeline_mode` is constrained to `'external'`. There are
no built-in pipeline drivers. The framework resolves the dotted path in
`ingest_adapter` at run time and calls `ingest_file(uri, *, file_name,
as_of_date, env, actor)`, which **must block** until the load finishes —
the runner asserts immediately after it returns. The adapter returns
`{file_id, status, error}`; `file_id` feeds the `{file_id}` assertion
placeholder, and `skipped_duplicate` lets idempotent pipelines short-circuit
replays.

### 5.3 Deterministic, namespaced fixtures

Generators are pure functions: same `(params, prefix)` → byte-identical
file, every run. Every generated worker id is namespaced under a per-run
prefix (`TC001-<run8>-W0000`), so merged batch loads and concurrent runs
can never collide. Poison generators (malformed, invalid, empty) exist
precisely to test the pipeline's reject handling.

### 5.4 Two ingest modes

- **per-file** (default): one adapter call per fixture file — maximum
  isolation, one pipeline launch per file.
- **batch**: eligible cases' files are merged by `as_of_date` into one
  NDJSON per date; one adapter call per group. Eligibility is automatic
  (flagged `batchable=FALSE`, multi-execution, poison fixtures, or
  byte-identical replay files stay solo). Assertion safety is preserved by
  contract: `{prefix}` is always per-case safe; `{file_id}` resolves to
  the shared batch load.

### 5.5 Assertions as data

Four kinds, all stored as JSONB in the case row:

- `sql_scalar` / `sql_row` — arbitrary SQL against the pipeline's database,
  `{placeholders}` bound as query parameters (never interpolated), compared
  with `eq/ne/gt/gte/lt/lte`. SQL errors become `error` results.
- `file_bytes` — storage round-trip fidelity (generated SHA-256 vs bytes
  read back through the backend).
- `file_rows` — file→table line accounting against the framework's bronze
  contract (SHA-256 raw-line identity); optional, for pipelines that adopt
  the contract.

### 5.6 Persistence-first results

Every run writes `tf.test_runs` (one row, with a `summary` JSONB carrying
totals, ingest mode, and the batch plan), `tf.test_case_results` (per
case/execution: fixture SHAs, adapter `file_id`s, statuses), and
`tf.assertion_results` (per assertion: expected vs actual, message). The
Markdown report is rendered from these rows after the run.

## 6. Run lifecycle

1. **Plan** — resolve suite/cases/category to case rows; derive the run
   prefix; take the per-environment advisory lock.
2. **Fixtures** — generate each case's NDJSON files from its
   `fixture_sequence`.
3. **Storage** — upload through the environment's backend; record URIs.
4. **Ingest** — per-file: call the adapter once per file. Batch: plan
   groups, merge, call once per group. Record `file_id`/`status` per call.
5. **Assert** — evaluate each case's expectations with the run context
   (`prefix`, `file_id[_N]`, `as_of[_N]`, `target_worker`, ...).
6. **Persist** — write the full result tree to `tf.*`.
7. **Report** — render `test_framework/reports/<run_id>.md`; release the
   lock.

`--dry-run` stops after planning (and prints the batch plan in batch mode).

## 7. Deployment view

The framework needs three things: PostgreSQL (the `tf.*` schema via
`scripts/migrate.sh`), a fixture bucket (local dir or GCS), and the
pipeline's adapter function. It runs anywhere Python runs — a laptop, a VM,
a Cloud Run Job (`deploy/runner/`), optionally triggered by the Composer
DAG (`deploy/composer/`). The framework's IAM footprint is minimal (Cloud
SQL client, GCS object admin on the fixture bucket); triggering the pipeline
is the adapter's own credentials, not the framework's.

## 8. Non-functional notes

- **Isolation:** per-run prefix namespacing + `tf-run:<env_id>` advisory
  lock serializing runs per environment.
- **Reproducibility:** deterministic fixtures + persisted plans, SHAs, and
  contexts; any run can be reconstructed from `tf.*`.
- **Failure handling:** assertion SQL errors, adapter exceptions, and bad
  fixtures are recorded as results, never raised; the run always completes
  and reports.
- **Scale lever:** batch mode trades per-file isolation for fewer pipeline
  launches; the eligibility rules and the `{prefix}`/`{file_id}` contract
  keep it sound.
- **Security:** placeholders are bound as query parameters (no SQL
  interpolation); the DSN travels via environment variable, never in rows.
