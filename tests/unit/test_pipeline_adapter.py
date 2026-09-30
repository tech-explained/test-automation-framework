"""Unit tests for the pipeline adapter (test_framework/pipeline_adapter.py).

The adapter is the single seam between the framework and the pipeline under
test. These tests cover contract loading/validation without touching a real
pipeline.
"""

from datetime import date

import pytest

from test_framework import pipeline_adapter as PA


def fake_ingest(uri, *, file_name, as_of_date, env, actor):
    return {"file_id": "fake-file-1", "status": "completed", "error": None}


def bad_shape_ingest(uri, *, file_name, as_of_date, env, actor):
    return {"nope": True}


def _env(**kw):
    base = {"env_id": "test", "pipeline_mode": "external", "ingest_adapter": None}
    base.update(kw)
    return base


def test_external_loads_dotted_path():
    ingest = PA.load_ingest(
        _env(ingest_adapter=f"{__name__}:fake_ingest"), {})
    res = ingest("gs://b/f.ndjson", file_name="f.ndjson",
                 as_of_date=date(2026, 9, 30), actor="t")
    assert res == {"file_id": "fake-file-1", "status": "completed", "error": None}


def test_external_passes_env_and_actor_through():
    seen = {}

    def spy(uri, *, file_name, as_of_date, env, actor):
        seen.update(uri=uri, file_name=file_name, as_of_date=as_of_date,
                    env_id=env["env_id"], actor=actor)
        return {"file_id": "x", "status": "completed", "error": None}

    globals()["spy"] = spy
    ingest = PA.load_ingest(_env(ingest_adapter=f"{__name__}:spy"), {})
    ingest("gs://b/f.ndjson", file_name="f.ndjson",
           as_of_date=date(2026, 9, 30), actor="actor-1")
    assert seen == {"uri": "gs://b/f.ndjson", "file_name": "f.ndjson",
                    "as_of_date": date(2026, 9, 30),
                    "env_id": "test", "actor": "actor-1"}


def test_external_missing_adapter_path_raises():
    with pytest.raises(RuntimeError, match="has no ingest_adapter"):
        PA.load_ingest(_env(), {})


def test_external_malformed_dotted_path_raises():
    with pytest.raises(RuntimeError, match="expected 'module.path:function_name'"):
        PA.load_ingest(_env(ingest_adapter="nodots"), {})


def test_external_unimportable_module_raises():
    with pytest.raises(RuntimeError, match="cannot import"):
        PA.load_ingest(_env(ingest_adapter="no_such_module_xyz:fake_ingest"), {})


def test_external_missing_function_raises():
    with pytest.raises(RuntimeError, match="has no 'missing_fn'"):
        PA.load_ingest(
            _env(ingest_adapter=f"{__name__}:missing_fn"), {})


def test_external_bad_return_shape_raises_on_call():
    ingest = PA.load_ingest(
        _env(ingest_adapter=f"{__name__}:bad_shape_ingest"), {})
    with pytest.raises(RuntimeError, match="must return a dict"):
        ingest("gs://b/f.ndjson", file_name="f.ndjson",
               as_of_date=date(2026, 9, 30), actor="t")


def test_unknown_mode_raises():
    with pytest.raises(RuntimeError, match="unknown pipeline_mode"):
        PA.load_ingest(_env(pipeline_mode="rocket"), {})


def test_non_external_mode_raises():
    # 'local'/'dataflow' built-in modes are gone; everything goes through
    # the external adapter seam.
    with pytest.raises(RuntimeError, match="only supports pipeline_mode='external'"):
        PA.load_ingest(_env(pipeline_mode="local"), {})
