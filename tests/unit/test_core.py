"""Unit tests for pipeline.core: parsing, hashing, dedupe, SCD4 decisions."""

import json
from datetime import date

import pytest

from pipeline import core


def _worker(wid="W-1", **over):
    w = {
        "Worker_ID": wid, "Employee_ID": "10001",
        "First_Name": "Aarav", "Last_Name": "Sharma",
        "Email": "aarav@example.com", "Hire_Date": "2020-01-15",
        "Worker_Status": "Active", "Department": "Data Platform",
        "Annual_Salary": 120000,
    }
    w.update(over)
    return w


class TestParseLine:
    def test_valid_object(self):
        obj, err = core.parse_line('{"Worker_ID": "W-1"}')
        assert err is None and obj["Worker_ID"] == "W-1"

    def test_blank_line(self):
        obj, err = core.parse_line("   \n")
        assert obj is None and err == "empty_line"

    def test_invalid_json(self):
        obj, err = core.parse_line('{"Worker_ID": ')
        assert obj is None and err.startswith("invalid_json")

    def test_non_object(self):
        obj, err = core.parse_line("[1, 2, 3]")
        assert obj is None and err == "not_a_json_object"

    def test_unicode_survives(self):
        obj, err = core.parse_line('{"Worker_ID": "W-9", "First_Name": "Zoë"}')
        assert err is None and obj["First_Name"] == "Zoë"


class TestWorkerId:
    def test_present(self):
        assert core.extract_worker_id({"Worker_ID": " W-1 "}) == "W-1"

    def test_missing(self):
        assert core.extract_worker_id({"Employee_ID": "1"}) is None

    def test_blank(self):
        assert core.extract_worker_id({"Worker_ID": "   "}) is None

    def test_numeric_coerced(self):
        assert core.extract_worker_id({"Worker_ID": 123}) == "123"


class TestRecordHash:
    def test_key_order_independent(self):
        a = _worker()
        b = dict(reversed(list(a.items())))
        assert core.record_hash(a) == core.record_hash(b)

    def test_change_detected(self):
        assert core.record_hash(_worker()) != core.record_hash(
            _worker(Department="Finance"))

    def test_unknown_fields_participate(self):
        assert core.record_hash(_worker()) != core.record_hash(
            _worker(Nickname="Aru"))

    def test_business_fields_equal(self):
        assert core.business_fields_equal(_worker(), dict(_worker()))
        assert not core.business_fields_equal(_worker(), _worker(Department="X"))

    def test_stable_across_runs(self):
        h1 = core.record_hash(_worker())
        h2 = core.record_hash(json.loads(json.dumps(_worker())))
        assert h1 == h2


class TestDedupe:
    def test_keep_last(self):
        rows = [(1, _worker("W-1", Department="A")),
                (2, _worker("W-2")),
                (3, _worker("W-1", Department="B"))]
        deduped, dup_count, dup_ids = core.dedupe_keep_last(rows)
        assert dup_count == 1 and dup_ids == ["W-1"]
        by_id = {core.extract_worker_id(o): (n, o) for n, o in deduped}
        assert by_id["W-1"][0] == 3
        assert by_id["W-1"][1]["Department"] == "B"
        assert len(deduped) == 2

    def test_no_dups(self):
        rows = [(1, _worker("W-1")), (2, _worker("W-2"))]
        deduped, dup_count, dup_ids = core.dedupe_keep_last(rows)
        assert (dup_count, dup_ids, len(deduped)) == (0, [], 2)


class TestDecideAction:
    D1 = date(2026, 9, 29)
    D2 = date(2026, 9, 30)

    def test_insert(self):
        assert core.decide_action(None, "h", self.D1) == "insert"

    def test_noop(self):
        cur = {"x": 1}
        assert core.decide_action(cur, "h", self.D2, self.D1, "h") == "noop"

    def test_update(self):
        cur = {"x": 1}
        assert core.decide_action(cur, "h2", self.D2, self.D1, "h1") == "update"

    def test_stale_never_regresses(self):
        cur = {"x": 1}
        assert core.decide_action(cur, "h2", self.D1, self.D2, "h1") == "stale"

    def test_same_asof_not_stale(self):
        cur = {"x": 1}
        assert core.decide_action(cur, "h2", self.D1, self.D1, "h1") == "update"


class TestFileId:
    def test_content_addressed(self):
        assert core.file_id_for_content("abc") == core.file_id_for_content("abc")
        assert core.file_id_for_content("abc") != core.file_id_for_content("abd")

    def test_as_of_from_filename(self):
        assert core.as_of_from_filename("workers_20260929.ndjson") == date(2026, 9, 29)
        assert core.as_of_from_filename("hr-extract-2026-09-29.ndjson") == date(2026, 9, 29)
        assert core.as_of_from_filename("nope.ndjson") is None
