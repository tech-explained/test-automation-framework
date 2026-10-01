"""Adapter template: how to plug YOUR data pipeline into the test framework.

Copy this file to your own module (e.g. ``mycompany/qa_adapter.py``),
implement ``ingest_file`` for your pipeline, and point
``tf.environments.ingest_adapter`` at its dotted path::

    mycompany.qa_adapter:ingest_file

The contract (see test_framework/pipeline_adapter.py)::

    ingest_file(uri, *, file_name, as_of_date, env, actor) -> dict

Args:
    uri: where the fixture file lives (GCS URI or local path, as produced
        by the framework's storage backend for this environment).
    file_name: basename of the fixture file.
    as_of_date: the business date for this load (datetime.date).
    env: the tf.environments row (dict) — connection info, bucket, etc.
    actor: string identifying who/what triggered the run (audit trail).

Returns:
    {"file_id": str, "status": "completed"|"failed"|"skipped_duplicate",
     "error": str | None}

Rules:
    * MUST block until the pipeline has finished loading the file. The
      framework runs assertions immediately after this returns.
    * ``file_id`` is YOUR pipeline's identity for this load (what you
      would use to trace it in your own tables/logs). The framework
      passes it back into assertions as ``{file_id}``.
    * On a duplicate/replayed file your pipeline skips, return
      ``status="skipped_duplicate"`` — the framework treats this as a
      successful idempotent no-op.
    * Never raise for a pipeline-level failure; return
      ``status="failed"`` with ``error`` set so the run records it.

Example skeleton (pseudocode — adapt to your orchestrator)::

    def ingest_file(uri, *, file_name, as_of_date, env, actor):
        # 1. Read any pipeline-specific launch settings:
        #    launch = (pipeline_row.get("launch_config") or {})
        # 2. Trigger your pipeline with:
        #      input_uri  = uri
        #      as_of_date = as_of_date.isoformat()
        #      actor      = actor
        # 3. Block until the load finishes (poll your orchestrator's
        #    API — do NOT return while the job is still running).
        # 4. Return your pipeline's own identity for the load:
        return {"file_id": "<your-job-or-batch-id>",
                "status": "completed", "error": None}
"""

from __future__ import annotations

from datetime import date
from typing import Any


def ingest_file(uri: str, *, file_name: str, as_of_date: date,
                env: dict, actor: str) -> dict[str, Any]:
    """Plug YOUR pipeline in here. This template intentionally does nothing."""
    raise NotImplementedError(
        "Copy examples/adapter_template.py to your own module and implement "
        "ingest_file() for your pipeline (see the docstring above), then set "
        "tf.environments.ingest_adapter to your module's dotted path."
    )
