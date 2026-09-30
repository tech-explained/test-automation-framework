"""Beam pipeline entrypoint: GCS NDJSON -> bronze Postgres.

Run on Dataflow (Flex Template) or locally with DirectRunner:

    python -m pipeline.main \
        --input_file gs://my-bucket/hr/raw/workers_20260929.ndjson \
        --file_id <uuid5-of-sha> --file_name workers_20260929.ndjson \
        --as_of_date 2026-09-29 \
        --pg_dsn_secret projects/123/secrets/hr-pg-dsn/versions/latest \
        --runner DataflowRunner --project my-gcp-project --region us-central1 \
        --temp_location gs://my-bucket/tmp/

The launcher (pipeline/launcher.py) normally invokes this; the silver SCD4
merge runs afterwards as a Postgres procedure, orchestrated by the launcher.
"""

from __future__ import annotations

import uuid
from datetime import date

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from pipeline.transforms import build_bronze_load, parse_args


def run(argv=None) -> None:
    opts, beam_args = parse_args(argv)
    pipeline_options = PipelineOptions(beam_args)

    with beam.Pipeline(options=pipeline_options) as p:
        build_bronze_load(
            p,
            input_file=opts.input_file,
            file_id=uuid.UUID(opts.file_id),
            file_name=opts.file_name,
            as_of_date=date.fromisoformat(opts.as_of_date),
            pg_dsn=opts.pg_dsn,
            pg_dsn_secret=opts.pg_dsn_secret,
            batch_size=opts.batch_size,
        )


if __name__ == "__main__":
    run()
