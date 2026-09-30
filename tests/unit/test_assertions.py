"""Unit tests for the assertion engine (SQL substitution + comparison)."""

import re
import uuid
from datetime import date

import psycopg
import pytest

from test_framework import assertions as A


@pytest.fixture()
def conn():
    import os
    with psycopg.connect(os.environ["HR_PG_DSN"]) as c:
        yield c


def _a(sql, **kw):
    d = {"name": "t", "kind": "sql_scalar", "sql": sql, "op": "eq",
         "expected": None}
    d.update(kw)
    return d


class TestSubstitute:
    def test_basic(self):
        sql, params = A.substitute(
            "SELECT count(*) FROM t WHERE worker_id LIKE {prefix} || '%'",
            {"prefix": "ABC"})
        assert params == {"prefix": "ABC"}
        assert "{prefix}" not in sql and "%(prefix)s" in sql

    def test_missing_placeholder(self):
        with pytest.raises(KeyError):
            A.substitute("SELECT {nope}", {})

    def test_injection_safe(self):
        evil = "x'; DROP TABLE tf.test_cases; --"
        sql, params = A.substitute("SELECT {prefix}", {"prefix": evil})
        assert evil not in sql  # goes through as a bound parameter


class TestRunAssertion:
    def test_scalar_pass(self, conn):
        res = A.run_assertion(conn, _a("SELECT 42", expected=42), {})
        assert res["status"] == "pass" and res["actual"] == 42

    def test_scalar_fail_message(self, conn):
        res = A.run_assertion(conn, _a("SELECT 1", expected=2), {})
        assert res["status"] == "fail"
        assert "2" in res["message"] and "1" in res["message"]

    def test_op_gte_float(self, conn):
        res = A.run_assertion(
            conn, _a("SELECT 0.6::float", op="gte", expected=0.6), {})
        assert res["status"] == "pass"

    def test_date_result_jsonable(self, conn):
        res = A.run_assertion(
            conn, _a("SELECT DATE '2026-09-29'", expected="2026-09-29"), {})
        assert res["status"] == "pass" and res["actual"] == "2026-09-29"

    def test_uuid_result_jsonable(self, conn):
        res = A.run_assertion(conn, _a("SELECT gen_random_uuid()",
                                       expected="anything"), {})
        assert res["status"] == "fail"  # not equal, but ran fine
        assert re.fullmatch(
            r"[0-9a-f-]{36}", res["actual"]), "uuid must be a plain string"

    def test_sql_error_becomes_error_status(self, conn):
        res = A.run_assertion(conn, _a("SELECT * FROM no_such_table_xyz"), {})
        assert res["status"] == "error"
        assert res["message"] is not None

    def test_row_kind(self, conn):
        res = A.run_assertion(
            conn, {"name": "t", "kind": "sql_row",
                   "sql": "SELECT 1 AS a, 'x' AS b", "op": "eq",
                   "expected": {"a": 1, "b": "x"}}, {})
        assert res["status"] == "pass"


class _FakeBackend:
    def __init__(self, blobs: dict):
        self._blobs = blobs  # uri -> bytes

    def upload_text(self, dest, content):
        self._blobs[dest] = content.encode("utf-8")
        return dest

    def read_bytes(self, uri):
        return self._blobs[uri]

    def describe(self):
        return "fake"


def _file_ctx(contents, uris, backend, file_ids):
    ctx = {"prefix": "UT", "file_contents": contents,
           "file_uris": uris, "backend": backend}
    for i, fid in enumerate(file_ids):
        ctx[f"file_id_{i}"] = fid
    if file_ids:
        ctx["file_id"] = file_ids[-1]
    return ctx


def _seed_bronze(conn, file_id, lines):
    """Mirror the loader: good lines -> events, bad lines -> rejects."""
    from pipeline import core as C
    with conn.cursor() as cur:
        for i, raw in enumerate(lines, start=1):
            if not raw.strip():
                continue
            obj, err = C.parse_line(raw)
            if err is None and C.extract_worker_id(obj) is None:
                err = "missing_worker_id"
            if err is None:
                cur.execute(
                    """INSERT INTO bronze.raw_worker_events
                           (file_id, file_name, line_no, as_of_date,
                            worker_json, row_hash)
                       VALUES (%s, 'ut.ndjson', %s, '2026-09-29',
                               %s::jsonb, %s)""",
                    (file_id, i, raw, C.row_hash(raw)))
            else:
                cur.execute(
                    """INSERT INTO bronze.raw_worker_rejects
                           (file_id, file_name, line_no, raw_line, error)
                       VALUES (%s, 'ut.ndjson', %s, %s, %s)""",
                    (file_id, i, raw[:8000], err))
    conn.commit()
    return [ln for ln in lines if ln.strip()]


class TestFileBytes:
    def test_roundtrip_ok(self, conn):
        content = '{"Worker_ID":"UT-W0000"}\n{"Worker_ID":"UT-W0001"}\n'
        backend = _FakeBackend({"mem://f0": content.encode("utf-8")})
        ctx = _file_ctx([content], ["mem://f0"], backend,
                        ["00000000-0000-0000-0000-000000000000"])
        res = A.run_assertion(
            conn, {"name": "t", "kind": "file_bytes", "file": "file_0",
                   "op": "eq", "expected": True}, ctx)
        assert res["status"] == "pass" and res["actual"] is True

    def test_roundtrip_corrupt_fails(self, conn):
        content = '{"Worker_ID":"UT-W0000"}\n'
        backend = _FakeBackend({"mem://f0": b'{"Worker_ID":"UT-W0000","x":1}\n'})
        ctx = _file_ctx([content], ["mem://f0"], backend,
                        ["00000000-0000-0000-0000-000000000000"])
        res = A.run_assertion(
            conn, {"name": "t", "kind": "file_bytes", "file": "file_0",
                   "op": "eq", "expected": True}, ctx)
        assert res["status"] == "fail" and res["actual"] is False

    def test_bad_file_ref_is_error(self, conn):
        backend = _FakeBackend({})
        ctx = _file_ctx(["x\n"], ["mem://f0"], backend, ["00000000-0000-0000-0000-000000000000"])
        res = A.run_assertion(
            conn, {"name": "t", "kind": "file_bytes", "file": "file_9",
                   "op": "eq", "expected": True}, ctx)
        assert res["status"] == "error"


class TestFileRows:
    def test_clean_file_fully_accounted(self, conn):
        from test_framework import fixtures as F
        fid = str(uuid.uuid4())
        lines = F.generate("initial_load", {"n": 4}, "UT")
        content = "\n".join(lines) + "\n"
        try:
            _seed_bronze(conn, fid, content.splitlines())
            backend = _FakeBackend({"mem://f0": content.encode("utf-8")})
            ctx = _file_ctx([content], ["mem://f0"], backend, [fid])
            res = A.run_assertion(
                conn, {"name": "t", "kind": "file_rows", "file": "file_0",
                       "op": "eq", "expected": 0}, ctx)
            assert res["status"] == "pass" and res["actual"] == 0
        finally:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM bronze.raw_worker_events WHERE file_id = %s", (fid,))
                cur.execute("DELETE FROM bronze.raw_worker_rejects WHERE file_id = %s", (fid,))
            conn.commit()

    def test_rejects_are_accounted_not_missing(self, conn):
        from test_framework import fixtures as F
        fid = str(uuid.uuid4())
        lines = F.generate("with_malformed", {"n": 3}, "UT")
        content = "\n".join(lines) + "\n"
        try:
            kept = _seed_bronze(conn, fid, content.splitlines())
            assert any("{" not in ln or "Worker_ID" not in ln for ln in kept), \
                "with_malformed fixture should contain bad lines"
            backend = _FakeBackend({"mem://f0": content.encode("utf-8")})
            ctx = _file_ctx([content], ["mem://f0"], backend, [fid])
            res = A.run_assertion(
                conn, {"name": "t", "kind": "file_rows", "file": "file_0",
                       "op": "eq", "expected": 0}, ctx)
            assert res["status"] == "pass" and res["actual"] == 0
        finally:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM bronze.raw_worker_events WHERE file_id = %s", (fid,))
                cur.execute("DELETE FROM bronze.raw_worker_rejects WHERE file_id = %s", (fid,))
            conn.commit()

    def test_missing_bronze_row_detected(self, conn):
        from test_framework import fixtures as F
        fid = str(uuid.uuid4())
        lines = F.generate("initial_load", {"n": 3}, "UT")
        content = "\n".join(lines) + "\n"
        try:
            _seed_bronze(conn, fid, content.splitlines())
            with conn.cursor() as cur:  # simulate a dropped line
                cur.execute(
                    """DELETE FROM bronze.raw_worker_events
                       WHERE file_id = %s AND line_no = 2""", (fid,))
            conn.commit()
            backend = _FakeBackend({"mem://f0": content.encode("utf-8")})
            ctx = _file_ctx([content], ["mem://f0"], backend, [fid])
            res = A.run_assertion(
                conn, {"name": "t", "kind": "file_rows", "file": "file_0",
                       "op": "eq", "expected": 0}, ctx)
            assert res["status"] == "fail" and res["actual"] == 1
        finally:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM bronze.raw_worker_events WHERE file_id = %s", (fid,))
                cur.execute("DELETE FROM bronze.raw_worker_rejects WHERE file_id = %s", (fid,))
            conn.commit()


class TestValidateFileKinds:
    def test_accepts_file_kinds(self):
        from test_framework import add_case as AC
        spec = {
            "test_case_id": "TC-999", "name": "n", "category": "audit",
            "description": "d", "pipeline_id": "p",
            "fixture_sequence": [
                {"generator": "initial_load", "params": {"n": 2},
                 "as_of_date": "2026-09-29"}],
            "assertions": [
                {"name": "a", "kind": "file_bytes", "file": "file_0",
                 "op": "eq", "expected": True},
                {"name": "b", "kind": "file_rows", "file": "file_0",
                 "op": "eq", "expected": 0}],
        }
        assert AC.validate(spec) == []

    def test_rejects_bad_file_ref_and_sql_mix(self):
        from test_framework import add_case as AC
        spec = {
            "test_case_id": "TC-999", "name": "n", "category": "audit",
            "description": "d", "pipeline_id": "p",
            "fixture_sequence": [
                {"generator": "initial_load", "params": {"n": 2},
                 "as_of_date": "2026-09-29"}],
            "assertions": [
                {"name": "a", "kind": "file_bytes", "file": "file_7",
                 "op": "eq", "expected": True},
                {"name": "b", "kind": "file_rows", "file": "file_0",
                 "sql": "SELECT 1", "op": "eq", "expected": 0}],
        }
        problems = AC.validate(spec)
        assert any("out of range" in p for p in problems)
        assert any("not 'sql'" in p for p in problems)
