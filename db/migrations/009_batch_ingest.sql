-- 009_batch_ingest.sql
-- Hybrid batch ingest: lets the runner merge batchable cases' fixture files
-- into one NDJSON per as_of_date and ingest each merged file once, instead
-- of one ingest call per fixture file.
--
-- batchable = FALSE for cases whose value depends on per-file load isolation:
--   * poison fixtures (malformed / invalid / missing ids): a bad file must
--     only ever sink its own load, never a shared batch
--   * the empty-file case: meaningless once merged into a non-empty file
-- Fresh installs get the right defaults from the seed files; this UPDATE
-- covers databases seeded before the column existed.

ALTER TABLE tf.test_cases
    ADD COLUMN IF NOT EXISTS batchable BOOLEAN NOT NULL DEFAULT TRUE;

UPDATE tf.test_cases SET batchable = FALSE
WHERE test_case_id = 'TC-002'  -- replay test: identical files, separate loads
   OR fixture_sequence::text LIKE '%"generator": "with_malformed"%'
   OR fixture_sequence::text LIKE '%"generator": "all_invalid"%'
   OR fixture_sequence::text LIKE '%"generator": "invalid_dates"%'
   OR fixture_sequence::text LIKE '%"generator": "missing_worker_id"%'
   OR fixture_sequence::text LIKE '%"generator": "empty"%';
