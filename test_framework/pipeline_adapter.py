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

Wiring: set ``tf.environments.pipeline_mode = 'external'`` and
``tf.environments.ingest_adapter`` to the dotted path of your adapter, e.g.
``mycompany.qa_adapter:ingest_file``. See ``examples/reference_adapter.py``
for a complete working example.

Note on ``file_rows``: that assertion kind verifies against the framework's
bronze contract (``test_framework.lineparse``: sha256-of-raw-line row
identity, ``Worker_ID`` natural key, 1-based ``line_no``). If the pipeline
under test uses different bronze identity semantics, prefer the
``sql_scalar`` / ``sql_row`` kinds against its own tables instead.
"""

from __future__ import annotations

import importlib
from datetime import date
from typing import Any, Callable


def _external_ingest(env: dict) -> Callable:
    dotted = (env.get("ingest_adapter") or "").strip()
    if not dotted:
        raise RuntimeError(
            f"environment '{env.get('env_id')}' has no ingest_adapter. Set "
            "tf.environments.ingest_adapter to the dotted path of your adapter, "
            "e.g. 'mycompany.qa_adapter:ingest_file'."
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
                f"'file_id' and 'status'; got: {result!r}"
            )
        return result

    return ingest


def load_ingest(env: dict, pipeline: dict) -> Callable[..., dict]:
    """Return the ingest callable for this environment's adapter."""
    mode = env.get("pipeline_mode")
    if mode != "external":
        raise RuntimeError(
            f"unknown pipeline_mode '{mode}' for environment '{env.get('env_id')}'. "
            "The framework only supports pipeline_mode='external' with "
            "tf.environments.ingest_adapter pointing at your adapter "
            "(see examples/reference_adapter.py)."
        )
    return _external_ingest(env)
