"""Unit tests for hybrid batch ingest (test_framework/batching.py + batch
paths in runner/assertions). No database required."""

from __future__ import annotations

import json
from datetime import date

from test_framework import assertions as A
from test_framework import batching as B
from test_framework import lineparse


def _f(case_id, fidx, as_of, lines, exec_no=1):
    return {"case_id": case_id, "exec_no": exec_no, "fidx": fidx,
            "as_of": as_of, "lines": lines}


# ---------------------------------------------------------------------------
# plan_batch_groups
# ---------------------------------------------------------------------------

def test_groups_by_date_sorted():
    files = [
        _f("TC-B", 0, date(2026, 9, 30), ['{"a":1}']),
        _f("TC-A", 0, date(2026, 9, 29), ['{"b":2}']),
        _f("TC-A", 1, date(2026, 9, 29), ['{"c":3}']),
    ]
    groups = B.plan_batch_groups(files)
    assert [g["as_of"] for g in groups] == [date(2026, 9, 29), date(2026, 9, 30)]
    g0 = groups[0]
    assert [(m["case_id"], m["fidx"]) for m in g0["members"]] == [
        ("TC-A", 0), ("TC-A", 1)]
    assert g0["content"] == '{"b":2}\n{"c":3}\n'


def test_offsets_are_exact_and_1_based():
    files = [
        _f("TC-A", 0, date(2026, 9, 29), ['{"a":1}', '{"a":2}']),
        _f("TC-B", 0, date(2026, 9, 29), ['{"b":1}']),
    ]
    (g,) = B.plan_batch_groups(files)
    assert g["offsets"] == {("TC-A", 1, 0): 1, ("TC-B", 1, 0): 3}
    # every member's slice of the merged content round-trips exactly
    lines = g["content"].splitlines()
    assert lines[0:2] == ['{"a":1}', '{"a":2}']
    assert lines[2:3] == ['{"b":1}']


def test_empty_group_list():
    assert B.plan_batch_groups([]) == []


# ---------------------------------------------------------------------------
# is_batch_eligible
# ---------------------------------------------------------------------------

def _case(**kw):
    base = {"test_case_id": "TC-X", "executions": 1, "batchable": True,
            "fixture_sequence": [{"generator": "initial_load",
                                  "params": {"n": 5},
                                  "as_of_date": "2026-09-29"}]}
    base.update(kw)
    return base


def test_eligible_happy_path():
    ok, reason = B.is_batch_eligible(_case(), ["a\n", "b\n"])
    assert ok, reason


def test_flag_false_forces_solo():
    ok, reason = B.is_batch_eligible(_case(batchable=False))
    assert not ok and "batchable=FALSE" in reason


def test_executions_gt_1_forces_solo():
    ok, reason = B.is_batch_eligible(_case(executions=2))
    assert not ok and "executions" in reason


def test_poison_generator_forces_solo():
    case = _case(fixture_sequence=[{"generator": "with_malformed",
                                    "params": {}, "as_of_date": "2026-09-29"}])
    ok, reason = B.is_batch_eligible(case)
    assert not ok and "with_malformed" in reason


def test_duplicate_content_forces_solo():
    ok, reason = B.is_batch_eligible(_case(), ["same\n", "same\n"])
    assert not ok and "replay" in reason


def test_no_contents_skips_dup_check():
    ok, _ = B.is_batch_eligible(_case())
    assert ok


# ---------------------------------------------------------------------------
# _eval_file_rows in batch mode (fake DB)
# ---------------------------------------------------------------------------

class _FakeCursor:
    """Canned bronze rows; applies the prefix LIKE filter like Postgres."""

    def __init__(self, events, rejects):
        # events: list of (row_hash, worker_id); rejects: list of line_no
        self._events = events
        self._rejects = rejects
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        if "raw_worker_events" in sql:
            rows = self._events
            if "LIKE" in sql:
                like = params[1]
                assert like.endswith("-%")
                stem = like[:-2]
                rows = [(h, w) for h, w in rows if w.startswith(stem + "-")]
            self._rows = [(h,) for h, _ in rows]
        else:
            self._rows = [(ln,) for ln in self._rejects]

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, events, rejects):
        self._events = events
        self._rejects = rejects

    def cursor(self):
        return _FakeCursor(self._events, self._rejects)


def _good_line(prefix, i):
    return json.dumps({"Worker_ID": f"{prefix}-W{i:04d}", "x": i})


def test_file_rows_batch_mode_scopes_to_case():
    prefix = "TC001-abc123"
    l1, l2 = _good_line(prefix, 0), _good_line(prefix, 1)
    bad = '{"Worker_ID": '  # invalid json
    content = "\n".join([l1, l2, bad]) + "\n"
    ctx = {"file_contents": [content], "file_uris": ["u"],
           "file_id": "fid", "file_id_0": "fid", "prefix": prefix,
           "batch_line_base_0": 6}
    h1, h2 = lineparse.row_hash(l1), lineparse.row_hash(l2)
    h_other = lineparse.row_hash(_good_line("TC009-abc123", 0))
    # shared file_id: other case's rows + other case's rejects present
    conn = _FakeConn(events=[(h1, f"{prefix}-W0000"),
                             (h2, f"{prefix}-W0001"),
                             (h_other, "TC009-abc123-W0000")],
                     rejects=[2, 8, 40])  # 8 == merged line of our bad line
    assertion = {"name": "rows", "kind": "file_rows", "file": "file_0",
                 "op": "eq", "expected": 0}
    actual, msg = A._eval_file_rows(conn, assertion, ctx)
    assert actual == 0, msg


def test_file_rows_batch_mode_detects_missing():
    prefix = "TC001-abc123"
    l1 = _good_line(prefix, 0)
    content = l1 + "\n"
    ctx = {"file_contents": [content], "file_uris": ["u"],
           "file_id": "fid", "prefix": prefix, "batch_line_base_0": 1}
    conn = _FakeConn(events=[], rejects=[])  # good line never landed
    assertion = {"name": "rows", "kind": "file_rows", "file": "file_0",
                 "op": "eq", "expected": 0}
    actual, msg = A._eval_file_rows(conn, assertion, ctx)
    assert actual == 1
    assert "missing from bronze" in msg


def test_file_rows_per_file_mode_unchanged():
    prefix = "TC001-abc123"
    l1 = _good_line(prefix, 0)
    content = l1 + "\n"
    ctx = {"file_contents": [content], "file_uris": ["u"],
           "file_id": "fid", "prefix": prefix}  # no batch_base
    conn = _FakeConn(events=[(lineparse.row_hash(l1), f"{prefix}-W0000")],
                     rejects=[])
    assertion = {"name": "rows", "kind": "file_rows", "file": "file_0",
                 "op": "eq", "expected": 0}
    actual, msg = A._eval_file_rows(conn, assertion, ctx)
    assert actual == 0, msg
