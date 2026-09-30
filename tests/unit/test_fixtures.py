"""Unit tests for fixture generators: determinism + contract with seeded cases."""

import json

from test_framework import fixtures as F


def test_generators_registered_match_seeds():
    import psycopg, os
    # every generator referenced by seeded test cases must exist
    dsn = os.environ["HR_PG_DSN"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT jsonb_array_elements(fixture_sequence)->>'generator' "
            "FROM tf.test_cases")
        used = {r[0] for r in cur.fetchall()}
    assert used <= set(F.GENERATORS), f"missing: {used - set(F.GENERATORS)}"


def test_deterministic_bytes():
    a = F.generate("initial_load", {"n": 5}, "TC001-x")
    b = F.generate("initial_load", {"n": 5}, "TC001-x")
    assert a == b


def test_shuffle_changes_bytes_not_data():
    from pipeline import core
    a = F.generate("initial_load", {"n": 5}, "P")
    b = F.generate("initial_load", {"n": 5, "shuffle": "reverse"}, "P")
    assert a != b  # different bytes -> different file_id
    ha = sorted(core.record_hash(json.loads(l)) for l in a)
    hb = sorted(core.record_hash(json.loads(l)) for l in b)
    assert ha == hb  # identical business data


def test_sequences_differ_only_in_intended_field():
    from pipeline import core
    base = {json.loads(l)["Worker_ID"]: json.loads(l)
            for l in F.generate("initial_load", {"n": 5}, "P")}
    changed = {json.loads(l)["Worker_ID"]: json.loads(l)
               for l in F.generate("dept_change", {"n": 5, "target_index": 2,
                                                   "new_department": "X"}, "P")}
    diffs = [wid for wid in base
             if not core.business_fields_equal(base[wid], changed[wid])]
    assert diffs == ["P-W0002"]
    assert changed["P-W0002"]["Department"] == "X"


def test_malformed_and_missing_generators():
    lines = F.generate("with_malformed", {"n": 5, "bad": 2}, "P")
    assert len(lines) == 7
    lines = F.generate("missing_worker_id", {"n": 4}, "P")
    assert len(lines) == 5
    assert "Worker_ID" not in json.loads(lines[-1])


def test_unicode_and_long_text():
    lines = F.generate("unicode_names", {"n": 3}, "P")
    assert json.loads(lines[1])["Preferred_Name"] == "李 伟"
    lines = F.generate("long_text", {"n": 5, "length": 5000}, "P")
    assert len(json.loads(lines[2])["Bio"]) == 5000


def test_empty_and_all_invalid():
    assert F.generate("empty", {}, "P") == []
    assert len(F.generate("all_invalid", {"count": 3}, "P")) == 3
