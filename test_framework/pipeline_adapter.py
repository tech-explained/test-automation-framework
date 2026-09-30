"""Pipeline adapter: the single seam between the test framework and the pipeline under test.

The framework only needs ONE thing from a pipeline: "take this fixture file
and load it, then tell me when you're done." Everything else (assertions,
results, reports) is just SQL against the database.

Adapter contract — a callable::

    ingest_file(uri, *, file_name, as_of_date, env, actor) -> dict

* ``uri``: fixture location the runner just uploaded (``gs://...`` or local path,
  depending on the environment's ``storage_backend``).
* ``file_name``: original file name, e.g. ``TC-001-exec1-part0.ndjson``.
* ``as_of_date``: business as-of date of the extract (a ``datetime.date``).
* ``env``: the ``tf.environments`` row as a dict.
* ``actor``: audit label, e.g. ``test-framework:<run_id>``.
* MUST block until the pipeline has finished loading the file — the runner
  evaluates assertions immediately after this returns.
* Returns ``{"file_id": str, "status": "completed"|"failed"|"skipped_duplicate",
  "error": str | None}``. ``file_id`` feeds the ``{file_id}`` assertion
  placeholder; use whatever file identity the pipeline under test has.

Modes (``tf.environments.pipeline_mode``):

* ``local`` / ``dataflow`` — built-in adapter wrapping the bundled reference
  pipeline's ``pipeline.launcher.ingest_file``. These are the only modes that
  import the ``pipeline`` package (lazily, inside the adapter).
* ``external`` — imports the dotted path in ``tf.environments.ingest_adapter``,
  e.g. ``mycompany.qa_adapter:ingest_file``. This is how a separately
  developed pipeline plugs in: implement the contract above, point the column
  at it, done. No changes to the framework.

Note on ``file_rows``: that assertion kind verifies against the framework's
bronze contract (``test_framework.lineparse``: sha256-of-raw-line row
identity, ``Worker_ID`` natural key, 1-based ``line_no``). If the external
pipeline's bronze layer uses different identity semantics, use the
``sql_scalar`` / ``sql_row`` kinds against its own tables instead.
"""

from __future__ import annotations

import importlib
import os
from datetime import date
from typing import Any, Callable


def _dataflow_cfg(env: dict, pipeline: dict) -> dict:
    secret = os.environ.get("DATAFLOW_PG_DSN_SECRET")
    if not secret:
        raise RuntimeError("DATAFLOW_PG_DSN_SECRET must be set for dataflow mode")
    return {
        "project": env["gcp_project"],
        "region": env["dataflow_region"],
        "template_gcs_path": pipeline["flex_template_gcs_path"],
        "pg_dsn_secret": secret,
    }


def _builtin_ingest(env: dict, pipeline: dict) -> Callable:
    """Adapter around the bundled reference pipeline's launcher."""
    from pipeline import launcher  # lazy: only built-in modes need it

    dataflow_cfg = _dataflow_cfg(env, pipeline) if env["pipeline_mode"] == "dataflow" else None

    def ingest(uri: str, *, file_name: str, as_of_date: date, actor: str) -> dict:
        return launcher.ingest_file(
            uri,
            file_name=file_name,
            as_of_date=as_of_date,
            mode=env["pipeline_mode"],
            actor=actor,
            dataflow=dataflow_cfg,
        )

    return ingest


def _external_ingest(env: dict) -> Callable:
    dotted = (env.get("ingest_adapter") or "").strip()
    if not dotted:
        raise RuntimeError(
            f"environment '{env.get('env_id')}' uses pipeline_mode='external' but "
            "tf.environments.ingest_adapter is not set. Set it to the dotted path "
            "of your adapter, e.g. 'mycompany.qa_adapter:ingest_file'."
        )
    if ":" not in dotted:
        raise RuntimeError(
            f"bad ingest_adapter '{dotted}': expected 'module.path:function_name'"
        )
    mod_name, func_name = dotted.rsplit(":", 1)
    try:
        mod = importlib.import_module(mod_name)
    except ImportError as exc:
        raise RuntimeError(f"cannot import ingest_adapter module '{mod_name}': {exc}")
    try:
        func = getattr(mod, func_name)
    except AttributeError:
        raise RuntimeError(
            f"ingest_adapter '{dotted}': module '{mod_name}' has no '{func_name}'"
        )
    if not callable(func):
        raise RuntimeError(f"ingest_adapter '{dotted}' is not callable")

    def ingest(uri: str, *, file_name: str, as_of_date: date, actor: str) -> dict:
        result = func(uri, file_name=file_name, as_of_date=as_of_date,
                      env=dict(env), actor=actor)
        if not isinstance(result, dict) or "file_id" not in result or "status" not in result:
            raise RuntimeError(
                f"ingest_adapter '{dotted}' must return a dict with at least "
                "'file_id' and 'status'; got: {result!r}"
            )
        return result

    return ingest


def load_ingest(env: dict, pipeline: dict) -> Callable[..., dict]:
    """Return the ingest callable for this environment's pipeline_mode."""
    mode = env.get("pipeline_mode")
    if mode in ("local", "dataflow"):
        return _builtin_ingest(env, pipeline)
    if mode == "external":
        return _external_ingest(env)
    raise RuntimeError(
        f"unknown pipeline_mode '{mode}' for environment '{env.get('env_id')}'. "
        "Expected 'local', 'dataflow' or 'external'."
    )
