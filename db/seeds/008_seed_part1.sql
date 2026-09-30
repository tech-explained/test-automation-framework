-- 008_seed_test_cases.sql
-- Seed environments, pipelines, suites and the 20-case QA matrix.
-- Idempotent: safe to re-run (ON CONFLICT DO NOTHING).

INSERT INTO tf.environments
    (env_id, display_name, pipeline_mode, storage_backend, gcp_project,
     gcs_bucket, local_bucket_root, dataflow_region, db_dsn_env_var,
     ingest_adapter, active)
VALUES
    ('local', 'Local dev (reference pipeline via example adapter + local disk as GCS)',
     'external', 'local', NULL, NULL, '/tmp/hr-gcs-local', 'us-central1', 'HR_PG_DSN',
     'examples.reference_adapter:ingest_file', TRUE),
    ('gcp', 'GCP (your Dataflow/Spark pipeline via your adapter + GCS)',
     'external', 'gcs', 'REPLACE_WITH_GCP_PROJECT', 'REPLACE_WITH_GCS_BUCKET',
     NULL, 'us-central1', 'HR_PG_DSN',
     'mycompany.qa_adapter:ingest_file', FALSE)
ON CONFLICT (env_id) DO NOTHING;

INSERT INTO tf.pipelines (pipeline_id, display_name, flex_template_gcs_path, pipeline_version, active)
VALUES ('hr-workday-ndjson-v1', 'Workday RaaS NDJSON -> bronze -> silver(SCD4) -> gold',
        'gs://REPLACE_WITH_BUCKET/templates/workday-hr-bronze.json', 'v1', TRUE)
ON CONFLICT (pipeline_id) DO NOTHING;

-- ============================ TC-001 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled, batchable)
VALUES ('TC-001', 'Happy path: initial load of 5 workers', 'functional',
 'Five brand-new workers load end to end: bronze rows, silver current rows, gold views, full lineage.',
 1,
 '[{"generator": "initial_load", "params": {"n": 5}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "bronze_row_count", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_events WHERE file_id = {file_id}::uuid AND worker_json ->> ''Worker_ID'' LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 5},
   {"name": "no_rejects", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_rejects WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": 0},
   {"name": "silver_current_count", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 5},
   {"name": "no_history_rows", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_history WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 0},
   {"name": "ingestion_completed", "kind": "sql_scalar",
    "sql": "SELECT status FROM ops.file_ingestions WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": "completed"},
   {"name": "gold_dim_count", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM gold.vw_worker_dim WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 5},
   {"name": "silver_lineage_to_file", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%'' AND source_file_id = {file_id}::uuid",
    "op": "eq", "expected": 5}
 ]}', TRUE, TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled,
  batchable = EXCLUDED.batchable;

-- ============================ TC-002 =====================================
-- batchable=FALSE: replay test, two identical files must load as separate events
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled, batchable)
VALUES ('TC-002', 'Idempotency: byte-identical file replayed', 'idempotency',
 'Same bytes ingested twice. Second ingest must short-circuit as skipped_duplicate: no new bronze rows, no silver changes, no history.',
 1,
 '[{"generator": "initial_load", "params": {"n": 5}, "as_of_date": "2026-09-29"}, {"generator": "initial_load", "params": {"n": 5}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "first_execution_completed", "kind": "sql_scalar",
    "sql": "SELECT status FROM ops.file_ingestions WHERE file_id = {file_id_0}::uuid",
    "op": "eq", "expected": "completed"},
   {"name": "replay_was_skipped", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.audit_log WHERE action = ''ingest.skipped_duplicate'' AND entity_id = {file_id_1}",
    "op": "eq", "expected": 1},
   {"name": "single_ingestion_row_per_content", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.file_ingestions WHERE file_sha256 = (SELECT file_sha256 FROM ops.file_ingestions WHERE file_id = {file_id_0}::uuid)",
    "op": "eq", "expected": 1},
   {"name": "bronze_not_duplicated", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_events WHERE file_id = {file_id_0}::uuid",
    "op": "eq", "expected": 5},
   {"name": "silver_still_5", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 5},
   {"name": "no_history_on_replay", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_history WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 0}
 ]}', TRUE, FALSE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled,
  batchable = EXCLUDED.batchable;

-- ============================ TC-003 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled, batchable)
VALUES ('TC-003', 'SCD4: worker department change creates one history row', 'scd4',
 'Second file moves one worker to a new department. Current row updates to version 2; exactly one history row keeps the old department with valid_to set.',
 1,
 '[{"generator": "initial_load", "params": {"n": 5}, "as_of_date": "2026-09-29"},
    {"generator": "dept_change", "params": {"n": 5, "target_index": 2, "new_department": "People Operations"}, "as_of_date": "2026-09-30"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "current_dept_updated", "kind": "sql_scalar",
    "sql": "SELECT department FROM silver.workers_current WHERE worker_id = {target_worker}",
    "op": "eq", "expected": "People Operations"},
   {"name": "version_bumped", "kind": "sql_scalar",
    "sql": "SELECT version FROM silver.workers_current WHERE worker_id = {target_worker}",
    "op": "eq", "expected": 2},
   {"name": "one_history_row", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_history WHERE worker_id = {target_worker}",
    "op": "eq", "expected": 1},
   {"name": "history_keeps_old_dept", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_history WHERE worker_id = {target_worker} AND department <> ''People Operations''",
    "op": "eq", "expected": 1},
   {"name": "history_valid_to_set", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_history WHERE worker_id = {target_worker} AND valid_to IS NOT NULL",
    "op": "eq", "expected": 1},
   {"name": "others_still_version_1", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%'' AND worker_id <> {target_worker} AND version = 1",
    "op": "eq", "expected": 4},
   {"name": "merge_audit_shows_one_update", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.audit_log WHERE action = ''silver.merge.completed'' AND entity_id = {file_id} AND (details->>''workers_updated'')::int = 1",
    "op": "eq", "expected": 1}
 ]}', TRUE, FALSE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled,
  batchable = EXCLUDED.batchable;

-- ============================ TC-004 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled, batchable)
VALUES ('TC-004', 'Idempotency: same business data in a NEW file (reordered lines)', 'idempotency',
 'Second file has identical worker data but shuffled line order (different bytes => new file_id). Hash-based change detection must produce zero history rows and keep version 1.',
 1,
 '[{"generator": "initial_load", "params": {"n": 5}, "as_of_date": "2026-09-29"},
    {"generator": "initial_load", "params": {"n": 5, "shuffle": "reverse"}, "as_of_date": "2026-09-30"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "both_files_completed", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.file_ingestions WHERE file_id IN ({file_id_0}::uuid, {file_id_1}::uuid) AND status = ''completed''",
    "op": "eq", "expected": 2},
   {"name": "no_history_rows", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_history WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 0},
   {"name": "all_still_version_1", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%'' AND version <> 1",
    "op": "eq", "expected": 0},
   {"name": "last_seen_advanced", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%'' AND last_seen_as_of_date = {as_of_1}::date",
    "op": "eq", "expected": 5}
 ]}', TRUE, TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled,
  batchable = EXCLUDED.batchable;

-- ============================ TC-005 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled, batchable)
VALUES ('TC-005', 'SCD4: termination is versioned, never deleted', 'scd4',
 'Worker status flips to Terminated with a termination date. Current row versions up; gold active roster excludes them; nothing is physically deleted.',
 1,
 '[{"generator": "initial_load", "params": {"n": 5}, "as_of_date": "2026-09-29"},
    {"generator": "terminate", "params": {"n": 5, "target_index": 1, "termination_date": "2026-09-28"}, "as_of_date": "2026-09-30"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "status_terminated", "kind": "sql_scalar",
    "sql": "SELECT worker_status FROM silver.workers_current WHERE worker_id = {target_worker}",
    "op": "eq", "expected": "Terminated"},
   {"name": "termination_date_set", "kind": "sql_scalar",
    "sql": "SELECT termination_date::text FROM silver.workers_current WHERE worker_id = {target_worker}",
    "op": "eq", "expected": "2026-09-28"},
   {"name": "version_2", "kind": "sql_scalar",
    "sql": "SELECT version FROM silver.workers_current WHERE worker_id = {target_worker}",
    "op": "eq", "expected": 2},
   {"name": "one_history_row", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_history WHERE worker_id = {target_worker}",
    "op": "eq", "expected": 1},
   {"name": "gold_excludes_from_active", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM gold.vw_worker_dim WHERE worker_id = {target_worker} AND is_active",
    "op": "eq", "expected": 0},
   {"name": "gold_active_roster_count", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM gold.vw_worker_dim WHERE worker_id LIKE {prefix} || ''-W%'' AND is_active",
    "op": "eq", "expected": 4},
   {"name": "row_not_deleted", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id = {target_worker}",
    "op": "eq", "expected": 1}
 ]}', TRUE, TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled,
  batchable = EXCLUDED.batchable;

-- ============================ TC-006 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled, batchable)
VALUES ('TC-006', 'SCD4: new hires insert without touching existing versions', 'scd4',
 'Second file adds two workers. Existing five stay at version 1 with zero history; the two new rows insert at version 1.',
 1,
 '[{"generator": "initial_load", "params": {"n": 5}, "as_of_date": "2026-09-29"},
    {"generator": "add_workers", "params": {"n": 5, "extra": 2}, "as_of_date": "2026-09-30"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "seven_current", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 7},
   {"name": "no_history", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_history WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 0},
   {"name": "new_workers_version_1", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id IN ({prefix} || ''-W0005'', {prefix} || ''-W0006'') AND version = 1",
    "op": "eq", "expected": 2}
 ]}', TRUE, TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled,
  batchable = EXCLUDED.batchable;

-- ============================ TC-007 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled, batchable)
VALUES ('TC-007', 'Negative: malformed JSON lines are quarantined', 'negative',
 'File mixes 5 good workers with 2 corrupt lines. Good rows load fully; bad lines land in bronze rejects; the run still completes.',
 1,
 '[{"generator": "with_malformed", "params": {"n": 5, "bad": 2}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "bronze_loaded_5", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_events WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": 5},
   {"name": "rejects_2", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_rejects WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": 2},
   {"name": "reject_reasons", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_rejects WHERE file_id = {file_id}::uuid AND error LIKE ''invalid_json%''",
    "op": "eq", "expected": 2},
   {"name": "silver_5", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 5},
   {"name": "ingestion_completed_with_rejects", "kind": "sql_scalar",
    "sql": "SELECT rows_rejected FROM ops.file_ingestions WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": 2}
 ]}', TRUE, FALSE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled,
  batchable = EXCLUDED.batchable;

-- ============================ TC-008 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled, batchable)
VALUES ('TC-008', 'Negative: row without Worker_ID is rejected', 'negative',
 'A worker object missing the natural key cannot be curated. It is quarantined with reason missing_worker_id; the other 4 workers load normally.',
 1,
 '[{"generator": "missing_worker_id", "params": {"n": 4}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "bronze_loaded_4", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_events WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": 4},
   {"name": "reject_reason", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_rejects WHERE file_id = {file_id}::uuid AND error = ''missing_worker_id''",
    "op": "eq", "expected": 1},
   {"name": "silver_4", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 4}
 ]}', TRUE, FALSE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled,
  batchable = EXCLUDED.batchable;
