-- 008_external_pipeline.sql
-- Decouple the test framework from the bundled reference pipeline.
--
-- 1. tf.environments.pipeline_mode gains 'external': the pipeline under test
--    is developed separately and plugs in via tf.environments.ingest_adapter.
-- 2. New column tf.environments.ingest_adapter: dotted path
--    'module.path:function_name' implementing the ingest contract documented in
--    test_framework/pipeline_adapter.py. Required when pipeline_mode='external';
--    ignored for the built-in 'local'/'dataflow' modes.

ALTER TABLE tf.environments DROP CONSTRAINT IF EXISTS environments_pipeline_mode_check;
ALTER TABLE tf.environments ADD CONSTRAINT environments_pipeline_mode_check
    CHECK (pipeline_mode IN ('local', 'dataflow', 'external'));

ALTER TABLE tf.environments ADD COLUMN IF NOT EXISTS ingest_adapter TEXT;

COMMENT ON COLUMN tf.environments.ingest_adapter IS
    'Dotted path module.path:function_name implementing the pipeline ingest '
    'contract (see test_framework/pipeline_adapter.py). Required when '
    'pipeline_mode=''external''; ignored for built-in local/dataflow modes.';
