# Low Level Design — Test Automation Framework

Companion to `HLD.md`. This document describes the framework's modules,
database schema, contracts, and key algorithms in implementation detail.

## 1. Module map

```
test_framework/
├── runner.py           # orchestrator (CLI entry point)
├── pipeline_adapter.py # the single pipeline seam
├── fixtures.py         # deterministic NDJSON generators (GENERATORS registry)
├── storage.py          # fixture storage backends (local / GCS)
├── assertions.py       # assertion engine (4 kinds)
├── batching.py         # batch ingest planning
├── lineparse.py        # canonical raw-line identity
├── reporting.py        # Markdown report builder
├── add_case.py         # test-case authoring helper (JSON spec → tf.test_cases)
└── db.py               # connection + migration helpers
```

Dependencies: `psycopg` (PostgreSQL), `google-cloud-storage` (only for the
GCS backend, imported lazily). No web framework, no ORM, no config files.

## 2. Database schema (`tf.*`)

Created by `db/migrations/001_test_framework.sql` (applied via
`scripts/migrate.sh`; idempotent). See `docs/schema-diagram.svg` for the
visual. Seven tables in two groups.

### 2.1 Test definitions (written by authors/operators)

**`tf.environments`** — where and how a run executes.

| Column | Type | Notes |
|---|---|---|
| `env_id` | TEXT PK | e.g. `local`, `gcp` |
| `display_name` | TEXT | human label |
| `pipeline_mode` | TEXT | always `'external'` (CHECK constraint); the framework has no built-in pipeline drivers |
| `storage_backend` | TEXT | `'local'` or `'gcs'` |
| `gcp_project`, `gcs_bucket`, `local_bucket_root`, `region` | TEXT | backend/config knobs; nullable per backend |
| `db_dsn_env_var` | TEXT | name of the env var holding the DSN (DSN never stored in rows) |
| `ingest_adapter` | TEXT | dotted path `module.path:function_name` — the pipeline plug-in point |
| `active` | BOOLEAN | only active envs can run |

**`tf.pipelines`** — registry of pipelines under test (metadata only; the
framework never executes pipeline code).

| Column | Type | Notes |
|---|---|---|
| `pipeline_id` | TEXT PK | e.g. `sample-pipeline-v1` |
| `display_name` | TEXT | |
| `launch_config` | JSONB | pipeline-specific launch settings for your adapter |
| `pipeline_version` | TEXT | |
| `active` | BOOLEAN | runner uses the single active row |

**`tf.suites`** — named, ordered case lists.

| Column | Type | Notes |
|---|---|---|
| `suite_id` | TEXT PK | e.g. `smoke`, `regression` |
| `display_name` | TEXT | |
| `test_case_ids` | JSONB | ordered array of `TC-NNN` ids |
| `active` | BOOLEAN | |

**`tf.test_cases`** — the test definition. This is the heart of the
metadata model.

| Column | Type | Notes |
|---|---|---|
| `test_case_id` | TEXT PK | `TC-NNN` format |
| `name`, `category`, `description` | TEXT | category ∈ functional/idempotency/scd4/negative/edge/audit/dq |
| `executions` | INTEGER | how many times the fixture sequence is loaded (default 1; >1 forces solo ingest) |
| `fixture_sequence` | JSONB | ordered list of `{generator, params, as_of_date}` — see §4 |
| `pipeline_id` | TEXT FK → `tf.pipelines` | which pipeline this case targets |
| `expectations` | JSONB | `{assertions: [...]}` — see §5 |
| `batchable` | BOOLEAN | FALSE forces solo ingest in batch mode |
| `enabled` | BOOLEAN | disabled cases are skipped by the resolver |
| `owner` | TEXT | |

### 2.2 Test execution (written by the runner)

**`tf.test_runs`** — one row per run.

| Column | Type | Notes |
|---|---|---|
| `run_id` | UUID PK | generated per run |
| `env_id` | TEXT FK → `tf.environments` | |
| `suite` | TEXT | suite label or `custom` |
| `status` | TEXT | `running` → `passed`/`failed` |
| `triggered_by` | TEXT | audit: who/what started the run |
| `started_at`, `finished_at` | TIMESTAMPTZ | |
| `summary` | JSONB | `{case_executions: {passed, failed, error}, ingest_mode, batch: {...}}` — the batch plan (groups, member cases, launch counts) is persisted here |

**`tf.test_case_results`** — one row per (run, case, execution).

| Column | Type | Notes |
|---|---|---|
| `run_id` | UUID FK → `tf.test_runs` | composite PK with the next two |
| `test_case_id` | TEXT FK → `tf.test_cases` | |
| `execution_no` | INTEGER | 1-based |
| `status` | TEXT | `running` → `passed`/`failed`/`error` |
| `gcs_uris` | JSONB | fixture file URIs (per-file) or manifest URIs (batch) |
| `file_ids` | JSONB | adapter-returned file identities, in fixture order |
| `load_statuses` | JSONB | adapter statuses per file |
| `started_at`, `finished_at` | TIMESTAMPTZ | |
| `details` | JSONB | assertion totals, error text + short traceback on `error` |

**`tf.assertion_results`** — one row per evaluated assertion.

| Column | Type | Notes |
|---|---|---|
| `assertion_id` | BIGSERIAL PK | |
| `run_id`, `test_case_id`, `execution_no` | | links to the case result (no FK constraint — append-only log semantics) |
| `name`, `kind` | TEXT | |
| `status` | TEXT | `pass`/`fail`/`error` |
| `expected`, `actual` | JSONB | JSON-normalized for comparability |
| `message` | TEXT | failure/error detail |
| `checked_at` | TIMESTAMPTZ | |

## 3. The ingest contract (`pipeline_adapter.py`)

```python
def ingest_file(uri, *, file_name, as_of_date, env, actor) -> dict:
```

- **Args:** `uri` (fixture location as produced by the storage backend),
  `file_name` (basename), `as_of_date` (business date, `datetime.date`),
  `env` (the `tf.environments` row as a dict), `actor` (audit string).
- **Returns:** `{"file_id": str, "status": "completed"|"failed"|"skipped_duplicate", "error": str|None}`.
- **Blocking contract:** MUST NOT return until the pipeline has finished
  loading the file. The runner evaluates assertions immediately after.
- **Resolution:** `load_ingest(env, pipeline)` imports the dotted path in
  `ingest_adapter` (`importlib`), validates `pipeline_mode == 'external'`,
  and wraps the call so adapter exceptions become recorded `error` results.
- **`file_id`** is the pipeline's own identity for the load (registry id,
  job id, ...). It feeds the `{file_id}` / `{file_id_N}` assertion
  placeholders.
- **`skipped_duplicate`** is the idempotency signal: the pipeline
  recognized a replay and skipped it. The runner treats it as success.

Template with a Dataflow Flex Template skeleton: `examples/adapter_template.py`.

## 4. Fixture generators (`fixtures.py`)

`GENERATORS: dict[str, Callable[[params, prefix], list[str]]]` maps names to
pure functions returning NDJSON lines (no trailing newline; the runner adds
it). Determinism rules:

- Same `(params, prefix)` → byte-identical output, every run.
- `make_worker(prefix, i, **overrides)` is the canonical record builder;
  worker `i` under a given prefix is identical across ALL generators, so a
  fixture *sequence* (e.g. `initial_load` → `dept_change`) differs only in
  the intended mutation.
- Every `Worker_ID` is `{prefix}-W{i:04d}` — the per-run namespace that
  makes batch merging safe.

Registered generators (17):

| Generator | Purpose |
|---|---|
| `initial_load` | happy-path snapshot of `n` workers |
| `add_workers` | snapshot with additional hires |
| `terminate` | snapshot with `target_index` terminated |
| `rehire` | terminated worker returns |
| `dept_change` | department mutation (versioning) |
| `promotion` | title + salary mutation |
| `location_change` | location mutation |
| `duplicate_rows` | same `Worker_ID` twice (last-wins) |
| `extra_fields` | unknown fields (schema drift) |
| `unicode_names` | non-ASCII names byte-exact |
| `long_text` | 5,000-char field (truncation) |
| `null_emails` | high null ratio (DQ advisory) |
| `with_malformed` | poison: malformed JSON lines mixed in |
| `missing_worker_id` | poison: rows without the natural key |
| `invalid_dates` | poison: unparseable dates |
| `all_invalid` | poison: zero parseable rows |
| `empty` | poison: zero-byte file |

A `fixture_sequence` entry is `{generator, params, as_of_date}`; the
runner generates one file per entry, in order, per execution.

## 5. Assertion engine (`assertions.py`)

### 5.1 Placeholder substitution

`substitute(sql_template, ctx)`:

1. Finds `{tokens}` via regex; raises `KeyError` on any token missing from
   the run context (fail-fast at bind time, recorded as `error`).
2. Escapes every literal `%` → `%%` (LIKE patterns, modulo) so psycopg
   never mistakes them for placeholders.
3. Rewrites tokens to `%(name)s` and returns `(sql, params)` — values are
   **bound as query parameters, never interpolated** (SQL-injection safe
   by construction).

Run context keys: `prefix`, `run_id`, `test_case_id`, `file_id` (last
file's), `file_id_0..N`, `as_of_0..N` (ISO dates), `target_worker`
(`{prefix}-W{target_index:04d}` when the last fixture step names one),
plus `file_contents`, `file_uris`, `backend` for the file kinds, and
`batch_line_base_N` in batch mode.

### 5.2 Kinds

- **`sql_scalar`**: runs the SQL, takes the first column of the first row,
  JSON-normalizes it (`_jsonable`: dates → ISO, Decimal → float, UUID →
  str, bytes → hex), compares with `op`.
- **`sql_row`**: same, but takes the whole first row as a dict
  (column-name → normalized value).
- **Comparators** (`OPS`): `eq/ne/gt/gte/lt/lte`. Numeric comparison is
  lenient across the JSON↔SQL boundary (int/float/Decimal interop);
  booleans compare as booleans.
- **`file_bytes`**: `{"file": "file_N"}` — SHA-256 of the runner-generated
  bytes vs SHA-256 of the bytes read back through the storage backend.
  Catches corruption between generation and ingest.
- **`file_rows`**: `{"file": "file_N"}` — every non-blank line accounted
  for. Parseable lines (with a `Worker_ID`) must appear in the pipeline's
  bronze events table matched by `row_hash` (SHA-256 of the raw line, per
  `lineparse.py`); unparseable/id-less lines must appear in the rejects
  table matched by 1-based `line_no`. Returns the unaccounted-line count
  (expected 0). In batch mode, good-row matching is scoped by worker
  prefix and reject lines use merged-file offsets (`batch_line_base_N`).
  This kind implements the framework's **bronze contract** — optional;
  pipelines with different identity semantics should use `sql_scalar` /
  `sql_row` instead.

### 5.3 Error semantics

`run_assertion` never raises: SQL errors, missing placeholders, bad kinds,
and comparison failures all become `{"status": "error"|"fail", "message":
...}` and are persisted via `persist_assertion`. A bad assertion can never
kill a run.

## 6. Batching (`batching.py`)

### 6.1 Eligibility — `is_batch_eligible(case, contents)`

A case rides the batch unless:

1. `batchable` is FALSE (solo by design — e.g. assertions needing
   file-level load isolation),
2. `executions > 1` (identical content must arrive as separate load events),
3. any fixture step uses a poison generator (`with_malformed`,
   `all_invalid`, `invalid_dates`, `missing_worker_id`, `empty`) — a
   hostile file must only ever sink its own load,
4. any two generated files are byte-identical (a replay/dedup test —
   merging would destroy the dedup semantics).

Returns `(eligible, reason)`; reasons are persisted in
`tf.test_runs.summary.batch.solo_reasons`.

### 6.2 Grouping — `plan_batch_groups(files)`

Groups files by `as_of_date` (the pipeline stamps one as-of per load;
merging across dates would corrupt date semantics). Within a group, member
files are concatenated **exactly** (`"\n".join(lines) + "\n"` per file),
and 1-based first-line offsets are recorded per
`(case_id, exec_no, fidx)` — so `file_rows` reject accounting stays exact
on the merged file.

### 6.3 Batch run flow (`runner._run_batch_phase1`)

1. Generate every eligible case's files; classify the rest as solo.
2. Upload per-case **manifest copies** (so `file_bytes` round-trip still
   works per file even though the ingested bytes live in merged files).
3. Plan groups; upload each merged NDJSON; call the adapter **once per
   group**.
4. Build `preingested[(case_id, exec_no, fidx)]` → `{uri (manifest),
   file_id (shared batch load), status, error, batch_base (line offset)}`.
5. `_run_one_case` then runs per case with `preingested` instead of calling
   the adapter — assertions see the shared `file_id` and the per-file
   `batch_line_base_N`.

### 6.4 Assertion contract for batch mode

- `{prefix}` — always safe (per-case worker namespace, never collides in
  a merged load).
- `{file_id}` / `{file_id_N}` — resolve to the **shared batch load's**
  file_id. Safe for "load completed" checks; NOT for per-case row counts —
  scope those by `{prefix}`.

## 7. Runner (`runner.py`)

CLI: `python -m test_framework.runner --env ENV (--suite S | --cases
A,B | --category C) [--ingest-mode per-file|batch] [--dry-run]
[--triggered-by X]`.

1. **Resolve** (`_resolve_cases`): suite → ordered ids from
   `tf.suites.test_case_ids`; `--cases` → explicit list; `--category` →
   enabled cases in that category. Missing/disabled ids are a hard error.
2. **Run row**: insert `tf.test_runs` (`status='running'`).
3. **Lock**: `pg_advisory_lock(hashtext('tf-run:<env_id>'))` — serializes
   runs against the same environment/database. Released in `finally`.
4. **Prefix**: `{TESTCASEID-without-dash}-{run_id[:8]}` per case — the
   isolation namespace.
5. **Per-file mode**: for each case → `_run_one_case` (generate → upload
   → adapter → assert → persist).
6. **Batch mode**: `_run_batch_phase1` (generate → merge → upload merged
   → adapter once per date group), then `_run_one_case` per case with
   `preingested` (batched) or live adapter calls (solo).
7. **Per case/execution** (`_run_one_case`):
   - insert `tf.test_case_results` (`status='running'`);
   - for each fixture step: generate → upload → adapter call (or reuse
     the batch result); on adapter `status='failed'` raise → caught below;
   - build the assertion context; evaluate each expectation via
     `assertions.run_assertion`; persist each via `persist_assertion`;
   - exceptions (adapter errors, assertion crashes) are caught per case:
     `status='error'`, details + short traceback stored, `conn.rollback()`,
     the run continues with the next case;
   - update the case-result row with statuses, URIs, file_ids, details.
8. **Finish**: update `tf.test_runs` (`passed`/`failed`, `summary` JSONB
   with totals + ingest mode + batch plan); build the Markdown report
   (`reporting.build_report` → `test_framework/reports/<run_id>.md`);
   unlock; exit 0/1.

`--dry-run` prints the plan (and, in batch mode, the full BATCH/SOLO
classification with reasons) without touching storage or the adapter.

## 8. Storage (`storage.py`)

Abstract `StorageBackend` with `upload_text(dest_path, content) -> uri`,
`read_bytes(uri) -> bytes`, `describe() -> str`.

- **`LocalStorageBackend(root)`**: treats a directory as the bucket;
  `upload_text` writes UTF-8 and returns the absolute path; `read_bytes`
  reads it back. Hermetic, no GCP.
- **`GCSBackend(bucket)`**: `google.cloud.storage` (lazy import);
  `upload_text` writes `application/x-ndjson` and returns
  `gs://bucket/path`; `read_bytes` downloads.
- `make_backend(kind, bucket=..., local_root=...)` selects by the
  environment's `storage_backend`. Artifact layout:
  `test-artifacts/<run_id>/<case_id>/exec<exec_no>/part-<fidx>.ndjson`
  (per-file) plus `test-artifacts/<run_id>/batch/<date>/merged.ndjson`
  and `.../batch-manifest/...` (batch).

## 9. Line identity (`lineparse.py`)

Canonical definitions shared by the framework's `file_rows` assertions:

- `parse_line(raw) -> (obj, err)`: JSON-parse one NDJSON line; `err` is
  set for malformed JSON.
- `extract_worker_id(obj)`: the natural key (`Worker_ID`); missing →
  treated as a reject line.
- `row_hash(raw)`: SHA-256 of the **raw line bytes** (not the parsed
  object) — byte-level identity, immune to key reordering.

A pipeline adopting the bronze contract stores `row_hash` per event row
and `line_no` (1-based over all lines including blanks) per reject row;
the framework's `file_rows` kind then verifies complete line accounting.

## 10. Case authoring (`add_case.py`)

`python -m test_framework.add_case --spec <json> [--apply]`.

- **Validate** (default): checks `test_case_id` format (`TC-NNN`),
  category membership, every `generator` exists in `GENERATORS`,
  `as_of_date` format, assertion shape per kind (SQL kinds need `sql`,
  file kinds need `file: "file_N"` and no `sql`), every `{placeholder}`
  in SQL is bindable (`prefix`, `file_id`, `target_worker`,
  `file_id_N`/`as_of_N` patterns), and file indices are in range.
- **Apply** (`--apply`): upserts `tf.test_cases` (re-running updates the
  case) and appends the id to each listed suite's `test_case_ids`
  (if not already present; suite must exist).
- Sample specs: `fixtures/samples/*.json`.

## 11. Reporting (`reporting.py`)

`build_report(conn, run_id)` reads **only** `tf.*` rows (the report is a
query, not a log scrape) and writes
`test_framework/reports/<run_id>.md`:

- header: suite, env, status, started/finished, triggered-by, case totals;
- per-case table: id, name, category, execution, status, ingested file
  ids, duration;
- failed/errored assertions section with expected vs actual and messages;
- auditability note pointing at the persisted rows.

## 12. Migrations (`db.py`, `scripts/migrate.sh`)

- `db.py`: `connect()` (DSN from the env var named by the environment
  row, default `HR_PG_DSN`) and migration helpers.
- `scripts/migrate.sh`: applies `db/migrations/*.sql` in order via
  `psql`; idempotent (`CREATE TABLE IF NOT EXISTS`, `ON CONFLICT DO
  NOTHING` in seeds). Framework-only: `001_test_framework.sql` creates
  the `tf` schema and the 7 tables.

## 13. Deployment modules

- **`deploy/runner/`**: `Dockerfile` + `cloudbuild.yaml` build the
  framework image; `job.yaml` defines the Cloud Run Job (service account,
  VPC connector for Cloud SQL private IP, `HR_PG_DSN` from Secret
  Manager, 2h timeout); `deploy.sh` builds, deploys, and executes the
  smoke suite. The image contains the framework + `examples/`; the
  operator's adapter package is copied in per the Dockerfile comments.
- **`deploy/composer/dags/qa_tests.py`**: DAG `qa_framework` —
  `qa_smoke` → `qa_regression` via `CloudRunExecuteJobOperator`,
  on-demand by default (`SCHEDULE = None`; set a cron for scheduled
  runs). Setup in `deploy/composer/README.md`.
- **`scripts/gcp_bootstrap.sh`**: one-time GCP infra (APIs, service
  account + IAM, Cloud SQL, GCS bucket, secrets, VPC connector);
  idempotent.

## 14. Extension points

| To... | Do this |
|---|---|
| Support a new pipeline | implement `ingest_file` (see `examples/adapter_template.py`), register the env row |
| Add a fixture shape | add a pure function to `GENERATORS` in `fixtures.py` |
| Add an assertion kind | extend `assertions.run_assertion` + `add_case.KINDS` |
| Add a storage backend | subclass `StorageBackend`, extend `make_backend` |
| Change batch rules | edit `batching.POISON_GENERATORS` / `is_batch_eligible` |
| New report format | extend `reporting.build_report` (reads only `tf.*`) |

## 15. Key invariants (do not break)

1. The framework never imports pipeline code; `pipeline_adapter.py` is
   the only module that resolves the adapter.
2. Placeholders are bound as query parameters — never string-interpolated
   into SQL.
3. Generators are pure functions of `(params, prefix)`.
4. Every worker id is namespaced under the run prefix.
5. `file_rows` accounting is exact: merged content is the exact
   concatenation of member files; offsets are 1-based and recorded.
6. A failing assertion or adapter call never aborts a run — it is
   recorded and the run continues.
7. The DSN travels via environment variable; it is never stored in `tf.*`.
