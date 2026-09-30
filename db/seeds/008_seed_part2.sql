-- 008_seed_part2.sql (continued): TC-009 .. TC-020 + suites.

-- ============================ TC-009 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled)
VALUES ('TC-009', 'Edge: duplicate worker rows inside one file (last wins)', 'edge',
 'Worker W0000 appears twice with different departments. Both raw lines stay in bronze; the merge dedupes keeping the LAST line; one current row, zero history, audit notes the duplicate.',
 1,
 '[{"generator": "duplicate_rows", "params": {"n": 5, "dup_index": 0, "dup_department": "Moonshot Lab"}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "bronze_keeps_both_lines", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_events WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": 6},
   {"name": "one_current_row", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 5},
   {"name": "last_line_wins", "kind": "sql_scalar",
    "sql": "SELECT department FROM silver.workers_current WHERE worker_id = {target_worker}",
    "op": "eq", "expected": "Moonshot Lab"},
   {"name": "no_history_from_dedupe", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_history WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 0},
   {"name": "audit_notes_duplicates", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.audit_log WHERE action = ''silver.merge.completed'' AND entity_id = {file_id} AND (details->>''duplicates_in_file'')::int >= 1",
    "op": "eq", "expected": 1},
   {"name": "dq_flags_duplicates", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.file_ingestions WHERE file_id = {file_id}::uuid AND dq_warnings::text LIKE ''%duplicate_workers_in_file%''",
    "op": "eq", "expected": 1}
 ]}', TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled;

-- ============================ TC-010 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled)
VALUES ('TC-010', 'Edge: unknown RaaS fields land in attributes (schema drift safe)', 'edge',
 'Workers carry fields the silver schema never heard of (Favorite_Snack, Nickname). Load succeeds and the extras are preserved verbatim in attributes.',
 1,
 '[{"generator": "extra_fields", "params": {"n": 5}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "silver_5", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 5},
   {"name": "extra_field_preserved", "kind": "sql_scalar",
    "sql": "SELECT attributes->>''Favorite_Snack'' FROM silver.workers_current WHERE worker_id = {prefix} || ''-W0000''",
    "op": "eq", "expected": "Samosa"},
   {"name": "no_rejects", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_rejects WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": 0}
 ]}', TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled;

-- ============================ TC-011 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled)
VALUES ('TC-011', 'Negative: unparseable dates become NULL, row still loads', 'negative',
 'Two workers have Hire_Date = ''not-a-date''. The safe parser nulls the field instead of failing the batch; a DQ warning is recorded.',
 1,
 '[{"generator": "invalid_dates", "params": {"n": 5}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "all_rows_load", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 5},
   {"name": "bad_dates_nulled", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%'' AND hire_date IS NULL",
    "op": "eq", "expected": 2},
   {"name": "dq_warns_invalid_date", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.file_ingestions WHERE file_id = {file_id}::uuid AND dq_warnings::text LIKE ''%invalid_hire_date%''",
    "op": "eq", "expected": 1}
 ]}', TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled;

-- ============================ TC-012 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled)
VALUES ('TC-012', 'Edge: unicode names survive byte-for-byte', 'edge',
 'Names with diacritics and CJK characters must round-trip through bronze JSONB and silver text columns exactly.',
 1,
 '[{"generator": "unicode_names", "params": {"n": 3}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "cjk_name_exact", "kind": "sql_scalar",
    "sql": "SELECT preferred_name FROM silver.workers_current WHERE worker_id = {prefix} || ''-W0001''",
    "op": "eq", "expected": "李 伟"},
   {"name": "diacritic_name_exact", "kind": "sql_scalar",
    "sql": "SELECT preferred_name FROM silver.workers_current WHERE worker_id = {prefix} || ''-W0000''",
    "op": "eq", "expected": "Zoë Müller"},
   {"name": "silver_3", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 3}
 ]}', TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled;

-- ============================ TC-013 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled)
VALUES ('TC-013', 'Edge: very long text values are not truncated', 'edge',
 'A 5000-character free-text field rides in attributes (JSONB). No silent truncation anywhere in the path.',
 1,
 '[{"generator": "long_text", "params": {"n": 5, "length": 5000}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "long_text_preserved", "kind": "sql_scalar",
    "sql": "SELECT LENGTH(attributes->>''Bio'') FROM silver.workers_current WHERE worker_id = {prefix} || ''-W0002''",
    "op": "eq", "expected": 5000},
   {"name": "silver_5", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 5}
 ]}', TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled;

-- ============================ TC-014 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled)
VALUES ('TC-014', 'Edge: empty file completes cleanly', 'negative',
 'A zero-line file is a valid no-op: ingestion completes, nothing lands in silver, DQ notes the empty file.',
 1,
 '[{"generator": "empty", "params": {}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "ingestion_completed", "kind": "sql_scalar",
    "sql": "SELECT status FROM ops.file_ingestions WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": "completed"},
   {"name": "zero_received", "kind": "sql_scalar",
    "sql": "SELECT rows_received FROM ops.file_ingestions WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": 0},
   {"name": "silver_untouched", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 0},
   {"name": "dq_notes_empty_file", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.file_ingestions WHERE file_id = {file_id}::uuid AND dq_warnings::text LIKE ''%empty_file%''",
    "op": "eq", "expected": 1}
 ]}', TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled;

-- ============================ TC-015 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled)
VALUES ('TC-015', 'Negative: file with only invalid lines', 'negative',
 'Every line is corrupt. The run still completes (nothing to merge is not a failure); all lines are quarantined; silver is untouched.',
 1,
 '[{"generator": "all_invalid", "params": {"count": 3}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "ingestion_completed", "kind": "sql_scalar",
    "sql": "SELECT status FROM ops.file_ingestions WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": "completed"},
   {"name": "nothing_loaded", "kind": "sql_scalar",
    "sql": "SELECT rows_loaded FROM ops.file_ingestions WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": 0},
   {"name": "all_rejected", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_rejects WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": 3},
   {"name": "silver_untouched", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 0},
   {"name": "merge_saw_zero_staged", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.audit_log WHERE action = ''silver.merge.completed'' AND entity_id = {file_id} AND (details->>''staged_workers'')::int = 0",
    "op": "eq", "expected": 1}
 ]}', TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled;

-- ============================ TC-016 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled)
VALUES ('TC-016', 'SCD4: stale (out-of-order) file never regresses silver', 'scd4',
 'A newer snapshot is processed first, then an older-dated file arrives. Bronze keeps both raw files; silver ignores every stale row.',
 1,
 '[{"generator": "initial_load", "params": {"n": 5}, "as_of_date": "2026-09-29"},
    {"generator": "dept_change", "params": {"n": 5, "target_index": 2, "new_department": "People Operations"}, "as_of_date": "2026-09-20"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "both_files_in_bronze", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_events WHERE file_id IN ({file_id_0}::uuid, {file_id_1}::uuid)",
    "op": "eq", "expected": 10},
   {"name": "stale_dept_not_applied", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id = {target_worker} AND department = ''People Operations''",
    "op": "eq", "expected": 0},
   {"name": "no_history_from_stale", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_history WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 0},
   {"name": "stale_rows_counted", "kind": "sql_scalar",
    "sql": "SELECT workers_skipped_stale FROM ops.file_ingestions WHERE file_id = {file_id_1}::uuid",
    "op": "gte", "expected": 1},
   {"name": "current_asof_stays_newer", "kind": "sql_scalar",
    "sql": "SELECT as_of_date::text FROM silver.workers_current WHERE worker_id = {target_worker}",
    "op": "eq", "expected": "2026-09-29"}
 ]}', TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled;

-- ============================ TC-017 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled)
VALUES ('TC-017', 'SCD4: terminate then rehire produces version 3 with 2 history rows', 'scd4',
 'Active -> Terminated -> Active across three snapshots. Full version chain: v1 (history), v2 Terminated (history), v3 Active (current).',
 1,
 '[{"generator": "initial_load", "params": {"n": 5}, "as_of_date": "2026-09-29"},
    {"generator": "terminate", "params": {"n": 5, "target_index": 1, "termination_date": "2026-09-28"}, "as_of_date": "2026-09-30"},
    {"generator": "rehire", "params": {"n": 5, "target_index": 1, "new_hire_date": "2026-10-01"}, "as_of_date": "2026-10-01"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "status_active_again", "kind": "sql_scalar",
    "sql": "SELECT worker_status FROM silver.workers_current WHERE worker_id = {target_worker}",
    "op": "eq", "expected": "Active"},
   {"name": "termination_cleared", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id = {target_worker} AND termination_date IS NULL",
    "op": "eq", "expected": 1},
   {"name": "version_3", "kind": "sql_scalar",
    "sql": "SELECT version FROM silver.workers_current WHERE worker_id = {target_worker}",
    "op": "eq", "expected": 3},
   {"name": "two_history_rows", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_history WHERE worker_id = {target_worker}",
    "op": "eq", "expected": 2},
   {"name": "terminated_version_in_history", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_history WHERE worker_id = {target_worker} AND worker_status = ''Terminated''",
    "op": "eq", "expected": 1}
 ]}', TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled;

-- ============================ TC-018 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled)
VALUES ('TC-018', 'Gold: point-in-time history view reconstructs past state', 'scd4',
 'After a department change, gold.vw_worker_history exposes both versions: the superseded one (is_current=false, old dept) and the current one.',
 1,
 '[{"generator": "initial_load", "params": {"n": 4}, "as_of_date": "2026-09-29"},
    {"generator": "dept_change", "params": {"n": 4, "target_index": 0, "new_department": "Moonshot Lab"}, "as_of_date": "2026-09-30"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "two_versions_in_gold_history", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM gold.vw_worker_history WHERE worker_id = {target_worker}",
    "op": "eq", "expected": 2},
   {"name": "old_version_flagged", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM gold.vw_worker_history WHERE worker_id = {target_worker} AND is_current = false AND department <> ''Moonshot Lab''",
    "op": "eq", "expected": 1},
   {"name": "current_version_flagged", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM gold.vw_worker_history WHERE worker_id = {target_worker} AND is_current = true AND department = ''Moonshot Lab''",
    "op": "eq", "expected": 1},
   {"name": "old_version_has_valid_to", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM gold.vw_worker_history WHERE worker_id = {target_worker} AND is_current = false AND valid_to IS NOT NULL",
    "op": "eq", "expected": 1}
 ]}', TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled;

-- ============================ TC-019 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled)
VALUES ('TC-019', 'Auditability: every artifact traces back to the file', 'audit',
 'Bronze rows carry file_name + line_no; ingestion, pipeline run, merge audit and test-framework result rows all exist and link together.',
 1,
 '[{"generator": "initial_load", "params": {"n": 3}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "bronze_lineage_complete", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM bronze.raw_worker_events WHERE file_id = {file_id}::uuid AND file_name IS NOT NULL AND line_no IS NOT NULL",
    "op": "eq", "expected": 3},
   {"name": "ingestion_timestamps", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.file_ingestions WHERE file_id = {file_id}::uuid AND status = ''completed'' AND started_at IS NOT NULL AND finished_at IS NOT NULL",
    "op": "eq", "expected": 1},
   {"name": "ingest_audit_pair", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.audit_log WHERE entity_id = {file_id} AND action IN (''ingest.started'', ''ingest.completed'')",
    "op": "eq", "expected": 2},
   {"name": "pipeline_run_recorded", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.pipeline_runs WHERE file_id = {file_id}::uuid AND status = ''completed''",
    "op": "eq", "expected": 1},
   {"name": "merge_audit_recorded", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.audit_log WHERE action = ''silver.merge.completed'' AND entity_id = {file_id}",
    "op": "eq", "expected": 1},
   {"name": "test_result_persisted", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM tf.test_case_results WHERE test_case_id = ''TC-019'' AND status IN (''running'', ''passed'', ''failed'')",
    "op": "gte", "expected": 1}
 ]}', TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled;

-- ============================ TC-020 =====================================
INSERT INTO tf.test_cases
    (test_case_id, name, category, description, executions, fixture_sequence,
     pipeline_id, expectations, enabled)
VALUES ('TC-020', 'DQ: high null-email ratio warns but never blocks the load', 'dq',
 '60% of workers miss Email. The load completes (warnings are advisory), silver has all 10 workers, and dq_warnings records the ratio.',
 1,
 '[{"generator": "null_emails", "params": {"n": 10, "null_ratio": 0.6}, "as_of_date": "2026-09-29"}]',
 'hr-workday-ndjson-v1',
 '{"assertions": [
   {"name": "ingestion_completed", "kind": "sql_scalar",
    "sql": "SELECT status FROM ops.file_ingestions WHERE file_id = {file_id}::uuid",
    "op": "eq", "expected": "completed"},
   {"name": "all_workers_loaded", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM silver.workers_current WHERE worker_id LIKE {prefix} || ''-W%''",
    "op": "eq", "expected": 10},
   {"name": "dq_warns_null_email", "kind": "sql_scalar",
    "sql": "SELECT COUNT(*) FROM ops.file_ingestions WHERE file_id = {file_id}::uuid AND dq_warnings::text LIKE ''%null_email_ratio%''",
    "op": "eq", "expected": 1},
   {"name": "dq_ratio_value", "kind": "sql_scalar",
    "sql": "SELECT (w->>''ratio'')::float FROM ops.file_ingestions, jsonb_array_elements(dq_warnings) AS w WHERE file_id = {file_id}::uuid AND w->>''check'' = ''null_email_ratio''",
    "op": "gte", "expected": 0.6}
 ]}', TRUE)
ON CONFLICT (test_case_id) DO UPDATE SET
  name = EXCLUDED.name, category = EXCLUDED.category,
  description = EXCLUDED.description, executions = EXCLUDED.executions,
  fixture_sequence = EXCLUDED.fixture_sequence,
  pipeline_id = EXCLUDED.pipeline_id,
  expectations = EXCLUDED.expectations, enabled = EXCLUDED.enabled;

-- ============================ suites =====================================
INSERT INTO tf.suites (suite_id, display_name, test_case_ids, active) VALUES
 ('smoke', 'Smoke: happy path + idempotency + one SCD4 + one negative',
  '["TC-001", "TC-002", "TC-003", "TC-007"]', TRUE),
 ('scd4', 'SCD Type 4 versioning suite',
  '["TC-003", "TC-004", "TC-005", "TC-006", "TC-009", "TC-016", "TC-017", "TC-018"]', TRUE),
 ('idempotency', 'Idempotency suite',
  '["TC-002", "TC-004", "TC-016"]', TRUE),
 ('negative', 'Negative and edge-case suite',
  '["TC-007", "TC-008", "TC-011", "TC-014", "TC-015"]', TRUE),
 ('regression', 'Full regression: all test cases',
  '["TC-001", "TC-002", "TC-003", "TC-004", "TC-005", "TC-006", "TC-007", "TC-008", "TC-009", "TC-010", "TC-011", "TC-012", "TC-013", "TC-014", "TC-015", "TC-016", "TC-017", "TC-018", "TC-019", "TC-020"]', TRUE)
ON CONFLICT (suite_id) DO NOTHING;
