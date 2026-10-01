# Test plan

## Philosophy

The framework treats QA as **data, not configuration**. Test cases, fixture
sequences, assertion SQL, environments, suites, and every result live in
PostgreSQL (`tf.*`); there are no YAML/JSON config files to drift. A test run
is fully reproducible from the database alone, and the report is a query, not
a log scrape.

## Layers

| Layer | Where | What it proves |
|---|---|---|
| Unit | `tests/unit` (pytest) | Line parsing + row identity, fixture determinism + contracts, assertion substitution/comparison, batch eligibility + grouping, adapter loading (dotted-path resolution, external-only mode) |
| E2E (framework) | `test_framework/runner.py` + `tf.*` | Full file → storage → adapter → database validation per test case |

## Suites

Suites are defined in `tf.suites` and group test cases by purpose. The
sample seeds (`examples/sample_seeds/`) define illustrative suites:

- **smoke**: happy path, replay idempotency, one versioning change,
  malformed input. Gate for every change.
- **regression**: the full sample case matrix.

## Running

```bash
python3 -m test_framework.runner --env local --suite smoke
python3 -m test_framework.runner --env local --suite regression
python3 -m test_framework.runner --env local --cases TC-003,TC-017
python3 -m test_framework.runner --env local --category scd4
python3 -m test_framework.runner --env local --suite regression --dry-run  # plan only
python3 -m test_framework.runner --env local --suite regression --ingest-mode batch
```

`--ingest-mode batch` merges batch-eligible cases by `as_of_date` and calls
the adapter once per group; solo cases still ingest individually.
`--dry-run --ingest-mode batch` prints the batch plan
(groups, launch count, and which cases go BATCH vs SOLO) without running.

## Sample test cases

`examples/sample_seeds/` ships illustrative test cases (TC-001..TC-023)
showing the framework's patterns: functional loads, idempotency replays,
versioning/SCD scenarios, negative inputs (malformed, missing keys, invalid
dates, empty files), edge cases (duplicates, unicode, long text, schema
drift), and audit/reconciliation checks.

`(solo)` marks cases that stay per-file in batch mode; the others are
batchable. Adapt every case's SQL to your own pipeline's tables — the
sample SQL references illustrative table names.

| ID | Category | Scenario | Batch |
|---|---|---|---|
| TC-001 | functional | Initial load: row counts, lineage, ingestion status | batch |
| TC-002 | idempotency | Byte-identical replay → `skipped_duplicate`, single ingestion record | solo |
| TC-003 | scd4 | Attribute change → new version, history row keeps old value | solo |
| TC-004 | idempotency | Same business data, reordered lines (new bytes) → no new versions | batch |
| TC-005 | scd4 | Termination → versioned, never deleted | batch |
| TC-006 | scd4 | New hires insert; existing versions untouched | batch |
| TC-007 | negative | Malformed JSON lines quarantined; valid rows load | solo |
| TC-008 | negative | Row without Worker_ID rejected | solo |
| TC-009 | edge | Duplicate Worker_ID in one file → last line wins, counted | batch |
| TC-010 | edge | Unknown input fields preserved (schema drift) | batch |
| TC-011 | negative | Unparseable dates → NULL, row still loads | solo |
| TC-012 | edge | Unicode names survive byte-for-byte | batch |
| TC-013 | edge | 5,000-char text not truncated | batch |
| TC-014 | negative | Empty file completes cleanly with DQ warning | solo |
| TC-015 | negative | All-invalid file → completed with zero staged workers | solo |
| TC-016 | scd4 | Stale (out-of-order) file never regresses current state | solo |
| TC-017 | scd4 | Terminate → rehire → version 3, 2 history rows | batch |
| TC-018 | scd4 | History view reconstructs past attribute values | batch |
| TC-019 | audit | Lineage: curated row → source file/line; audit covers started/completed | batch |
| TC-020 | dq | 60% null emails → advisory warning, load not blocked | solo |
| TC-021 | scd4 | Promotion: title + salary change versions the worker; old title kept in history | batch |
| TC-022 | scd4 | Location change versions the worker; old location kept in history | batch |
| TC-023 | audit | Reconciliation: file content matches curated table, zero drift | solo |

The sample cases target illustrative table names; for your own pipeline,
author cases against your own tables with the same four assertion kinds.

## Batch mode

`--ingest-mode per-file` (default) calls the adapter once per fixture file —
maximum isolation. `--ingest-mode batch` merges batch-eligible cases'
fixture files by `as_of_date` into one NDJSON per date and calls the adapter
once per group.

Eligibility (`test_framework/batching.py:is_batch_eligible`): a case rides
the batch unless

- `tf.test_cases.batchable` is FALSE (solo by design), or
- `executions > 1` (identical content must arrive as separate load events), or
- its fixture sequence uses a poison generator (`with_malformed`,
  `all_invalid`, `invalid_dates`, `missing_worker_id`, `empty`), or
- its generated fixture files are byte-identical (a replay/dedup test).

Grouping (`plan_batch_groups`) concatenates member files per date exactly,
so per-case line offsets stay exact for reject accounting. The full batch
plan — groups, member cases, launch counts — is persisted in
`tf.test_runs.summary`.

**Assertion rule for batch mode**: `{prefix}` is always safe (per-case worker
namespace, never collides across a merged load). `{file_id}` /
`{file_id_N}` resolve to the *shared batch load's* file_id — fine for "load
completed" checks, **not** for per-case row counts; scope those by
`{prefix}` instead. When in doubt, `--dry-run --ingest-mode batch` shows
exactly what shares a load.

## Isolation model

Each run derives a unique worker prefix (`TC001-<run_short>`), so cases and
runs never contaminate each other — even when their lines share one merged
batch load. One advisory lock per environment (`tf-run:<env_id>`)
serializes runs against the same database.

## Assertion engine

Four kinds. `sql_scalar` / `sql_row` take `sql` with `{placeholders}` bound
from the run context (`prefix`, `file_id`, `file_id_0…N`, `as_of_0…N`,
`target_worker`). Placeholders are bound as query parameters — never
interpolated — and literal `%` in SQL (LIKE patterns) is escaped. Comparators:
`eq/ne/gt/gte/lt/lte`. SQL errors become `error` results, never run-killers.

Two file kinds are evaluated in Python by the runner (which holds the exact
fixture bytes) and take `file: "file_N"` (0-based fixture index) instead
of `sql`:

- `file_bytes` — storage round-trip fidelity: SHA-256 of the generated bytes
  vs SHA-256 of the bytes read back through the storage backend. `expected`
  is boolean. Catches corruption between fixture generation and ingest.
- `file_rows` — file→table line accounting: every non-blank line must be
  accounted for. Parseable lines (with a `Worker_ID`) must appear in the
  pipeline's events table matched by `row_hash` (SHA-256 of the raw line,
  exactly as computed in `test_framework/lineparse.py`); unparseable /
  id-less lines must appear in the rejects table matched by 1-based
  `line_no`. `expected` is the unaccounted-line count (0). Handles files
  with intentional rejects — they count as accounted, not missing. In batch
  mode, good-row matching is scoped by worker prefix and reject lines use
  merged-file offsets, so per-case accounting still holds on shared loads.

## Environments

- **local**: local "bucket" dir + your adapter. No GCP. Used for
  development and CI.
- **gcp**: real GCS bucket + your pipeline via your adapter
  (`tf.environments.ingest_adapter = 'mycompany.qa_adapter:ingest_file'`,
  `pipeline_mode = 'external'`).

## Adding a test case

Write a plain JSON spec and run the helper (file kinds need no SQL at all):

```bash
# 1. write fixtures/samples/tc-024-new-case.json (see the tc-021 sample for the format)
# 2. validate (dry run)
python3 -m test_framework.add_case --spec fixtures/samples/tc-024-new-case.json
# 3. write to tf.test_cases + link into suites
python3 -m test_framework.add_case --spec fixtures/samples/tc-024-new-case.json --apply
# 4. run it
python3 -m test_framework.runner --env local --cases TC-024
```

The helper validates the spec before anything touches the database:
test-case id format, category, that every `generator` name exists in
`test_framework/fixtures.py`, assertion shape (`kind`/`op`; SQL kinds need
`sql`, file kinds need `file: "file_N"` and take no `sql`), that every
`{placeholder}` in SQL is one the runner can bind (`{prefix}`,
`{file_id}`, `{file_id_0..N}`, `{as_of_0..N}`, `{target_worker}`), and that
every named suite exists. `batchable` is optional in the spec (defaults to
TRUE); set it to `false` for cases whose value depends on per-file load
isolation. `--apply` upserts `tf.test_cases` (re-running it updates the
case) and appends the case id to each listed suite's `test_case_ids`.
