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
    with pytest.raises(RuntimeError, match="ingest_adapter is not set"):
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


def test_builtin_local_wraps_launcher(monkeypatch):
    from pipeline import launcher

    calls = {}

    def fake_launcher_ingest(uri, *, file_name, as_of_date, mode, actor, dataflow):
        calls.update(uri=uri, mode=mode, dataflow=dataflow, actor=actor)
        return {"file_id": "built-in-1", "status": "completed", "error": None}

    monkeypatch.setattr(launcher, "ingest_file", fake_launcher_ingest)
    ingest = PA.load_ingest({"env_id": "local", "pipeline_mode": "local"}, {})
    res = ingest("file.ndjson", file_name="f.ndjson",
                 as_of_date=date(2026, 9, 30), actor="t")
    assert res["file_id"] == "built-in-1"
    assert calls["mode"] == "local"
    assert calls["dataflow"] is None
    assert calls["actor"] == "t"


def test_builtin_dataflow_builds_cfg(monkeypatch):
    import os
    from pipeline import launcher

    monkeypatch.setenv("DATAFLOW_PG_DSN_SECRET", "hr-postgres-dsn")
    seen = {}
    monkeypatch.setattr(
        launcher, "ingest_file",
        lambda uri, **kw: (seen.update(kw), {"file_id": "df-1", "status": "completed", "error": None})[1])
    env = {"env_id": "gcp", "pipeline_mode": "dataflow",
           "gcp_project": "p", "dataflow_region": "us-central1"}
    ingest = PA.load_ingest(env, {"flex_template_gcs_path": "gs://b/t.json"})
    ingest("gs://b/f.ndjson", file_name="f.ndjson",
           as_of_date=date(2026, 9, 30), actor="t")
    assert seen["mode"] == "dataflow"
    assert seen["dataflow"]["pg_dsn_secret"] == "hr-postgres-dsn"
    assert seen["dataflow"]["template_gcs_path"] == "gs://b/t.json"


def test_builtin_dataflow_missing_secret_raises(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PG_DSN_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="DATAFLOW_PG_DSN_SECRET"):
        PA.load_ingest({"env_id": "gcp", "pipeline_mode": "dataflow"}, {})
