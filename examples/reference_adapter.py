"""Example pipeline adapter: how to plug a data pipeline into the test framework.

This implements the framework's ingest contract using the reference pipeline
in ``examples/reference_pipeline`` (local in-process execution). It exists so
you can run the framework end-to-end with zero infrastructure and so you have
a concrete template for wiring YOUR pipeline.

The contract (see test_framework/pipeline_adapter.py)::

    ingest_file(uri, *, file_name, as_of_date, env, actor) -> dict

* MUST block until the pipeline has finished loading the file.
* Returns {"file_id": str, "status": "completed"|"failed"|"skipped_duplicate",
  "error": str | None}.

To plug in a real pipeline (Dataflow, Spark, dbt, ...), copy this file and
replace the body: trigger your job, wait for it to finish, and return the
file identity YOUR pipeline uses. Point tf.environments.ingest_adapter at
your module, e.g. 'mycompany.qa_adapter:ingest_file'.
"""

from __future__ import annotations

from datetime import date
from typing import Any


def ingest_file(uri: str, *, file_name: str, as_of_date: date,
                env: dict, actor: str) -> dict[str, Any]:
    """Run the reference pipeline locally over one fixture file."""
    from examples.reference_pipeline import launcher

    result = launcher.ingest_file(
        uri,
        file_name=file_name,
        as_of_date=as_of_date,
        mode="local",
        actor=actor,
    )
    return {
        "file_id": str(result["file_id"]),
        "status": result["status"],
        "error": result.get("error"),
    }
