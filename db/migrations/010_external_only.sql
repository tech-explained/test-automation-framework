-- 010_external_only.sql
-- The framework is standalone: the pipeline under test is ALWAYS external.
-- The old bundled 'local'/'dataflow' modes (which imported the reference
-- pipeline) are gone; the reference pipeline now lives in examples/ and is
-- wired through the same external seam as any real pipeline.

ALTER TABLE tf.environments DROP CONSTRAINT IF EXISTS environments_pipeline_mode_check;

-- Local dev runs the reference pipeline via the example adapter.
UPDATE tf.environments
SET pipeline_mode = 'external',
    ingest_adapter = 'examples.reference_adapter:ingest_file'
WHERE env_id = 'local';

-- The GCP environment row is a template: point ingest_adapter at your own
-- Dataflow/Spark/dbt adapter (see examples/reference_adapter.py).
UPDATE tf.environments
SET pipeline_mode = 'external',
    ingest_adapter = 'mycompany.qa_adapter:ingest_file'
WHERE env_id = 'gcp' AND (ingest_adapter IS NULL OR ingest_adapter = '');

ALTER TABLE tf.environments ADD CONSTRAINT environments_pipeline_mode_check
    CHECK (pipeline_mode = 'external');

COMMENT ON COLUMN tf.environments.ingest_adapter IS
    'Dotted path module.path:function_name implementing the pipeline ingest '
    'contract (see test_framework/pipeline_adapter.py and '
    'examples/reference_adapter.py).';
