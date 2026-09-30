"""Deterministic NDJSON fixture generators for the QA framework.

Every generator is a pure function of (params, prefix): same inputs produce
byte-identical files on every run, which is what makes replay/idempotency
tests meaningful. Workers for a given (prefix, index) are ALWAYS identical
across generators, so a fixture *sequence* (e.g. initial_load -> dept_change)
differs only in the intended mutation.

Registered by name in GENERATORS; test cases reference them from
tf.test_cases.fixture_sequence (metadata, not config files).
"""

from __future__ import annotations

import json
import math
from typing import Callable

FIRST = ["Aarav", "Diya", "Kabir", "Meera", "Arjun", "Isha",
         "Vikram", "Ananya", "Rohan", "Priya", "Aditya", "Sneha"]
LAST = ["Sharma", "Patel", "Reddy", "Iyer", "Gupta", "Nair",
        "Khan", "Das", "Menon", "Rao", "Joshi", "Chopra"]
DEPTS = ["Data Platform", "People Operations", "Finance", "Sales", "Marketing"]
PROFILES = ["Data Engineer", "Senior Data Engineer", "HR Business Partner",
            "Financial Analyst", "Account Executive", "Marketing Manager"]
UNICODE_PAIRS = [("Zoë", "Müller"), ("李", "伟"), ("José", "García")]


def make_worker(prefix: str, i: int, **overrides) -> dict:
    """Canonical worker for (prefix, i). Deterministic by construction."""
    first, last = FIRST[i % len(FIRST)], LAST[i % len(LAST)]
    w = {
        "Worker_ID": f"{prefix}-W{i:04d}",
        "Employee_ID": str(10000 + i),
        "First_Name": first,
        "Last_Name": last,
        "Preferred_Name": f"{first} {last}",
        "Email": f"{first}.{last}{i}@example.com".lower(),
        "Hire_Date": f"{2015 + (i % 8)}-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
        "Termination_Date": None,
        "Worker_Status": "Active",
        "Job_Profile": PROFILES[i % len(PROFILES)],
        "Job_Family": "Corporate",
        "Department": DEPTS[i % len(DEPTS)],
        "Department_ID": f"D-{100 + (i % len(DEPTS))}",
        "Location": "Dallas, TX",
        "Country": "USA",
        "Manager_Worker_ID": f"{prefix}-W0000" if i > 0 else None,
        "Cost_Center": f"CC-{(i % 4) + 1}",
        "Employment_Type": "Full Time",
        "Worker_Type": "Employee",
        "Time_Type": "Full time",
        "Compensation_Grade": f"G{(i % 5) + 5}",
        "Annual_Salary": 90000 + i * 2500,
        "Currency": "USD",
    }
    w.update(overrides)
    return w


def _ndjson(workers: list[dict]) -> list[str]:
    return [json.dumps(w, ensure_ascii=False) for w in workers]


# ---------------------------------------------------------------------------
# generators: each takes (params: dict, prefix: str) -> list[str] (NDJSON lines)
# ---------------------------------------------------------------------------

def gen_initial_load(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 5))
    workers = [make_worker(prefix, i) for i in range(n)]
    if params.get("shuffle") == "reverse":
        workers = workers[::-1]
    return _ndjson(workers)


def gen_dept_change(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 5))
    target = int(params["target_index"])
    new_dept = params["new_department"]
    workers = [
        make_worker(prefix, i, Department=new_dept) if i == target else make_worker(prefix, i)
        for i in range(n)
    ]
    return _ndjson(workers)


def gen_terminate(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 5))
    target = int(params["target_index"])
    term_date = params.get("termination_date", "2026-09-28")
    workers = [
        make_worker(prefix, i, Worker_Status="Terminated", Termination_Date=term_date)
        if i == target else make_worker(prefix, i)
        for i in range(n)
    ]
    return _ndjson(workers)


def gen_rehire(params: dict, prefix: str) -> list[str]:
    # A rehire is NOT byte-identical to the original load: the worker comes
    # back Active with a fresh hire date (otherwise content-addressed
    # idempotency would correctly treat it as a replay).
    n = int(params.get("n", 5))
    target = params.get("target_index")
    new_hire_date = params.get("new_hire_date")
    workers = [make_worker(prefix, i) for i in range(n)]
    if target is not None:
        t = int(target)
        workers[t]["Hire_Date"] = new_hire_date or workers[t]["Hire_Date"]
        workers[t]["Worker_Status"] = "Active"
        workers[t]["Termination_Date"] = None
    return _ndjson(workers)


def gen_add_workers(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 5))
    extra = int(params.get("extra", 2))
    return _ndjson([make_worker(prefix, i) for i in range(n + extra)])


def gen_with_malformed(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 5))
    bad = int(params.get("bad", 2))
    lines = _ndjson([make_worker(prefix, i) for i in range(n)])
    lines.append('{"Worker_ID": "BROKEN-1", "First_Name": ')   # truncated JSON
    lines.append('this is not json at all')                    # not JSON
    return lines[: n + bad]


def gen_missing_worker_id(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 4))
    workers = [make_worker(prefix, i) for i in range(n)]
    orphan = make_worker(prefix, 999)
    del orphan["Worker_ID"]
    workers.append(orphan)
    return _ndjson(workers)


def gen_duplicate_rows(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 5))
    dup_index = int(params.get("dup_index", 0))
    dup_dept = params.get("dup_department", "Moonshot Lab")
    workers = [make_worker(prefix, i) for i in range(n)]
    workers.append(make_worker(prefix, dup_index, Department=dup_dept))  # last wins
    return _ndjson(workers)


def gen_extra_fields(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 5))
    workers = [make_worker(prefix, i) for i in range(n)]
    workers[0]["Favorite_Snack"] = "Samosa"
    workers[0]["Nickname"] = "Aru"
    return _ndjson(workers)


def gen_invalid_dates(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 5))
    workers = [make_worker(prefix, i) for i in range(n)]
    for i in (1, 3):
        if i < n:
            workers[i]["Hire_Date"] = "not-a-date"
    return _ndjson(workers)


def gen_unicode_names(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 3))
    workers = [make_worker(prefix, i) for i in range(n)]
    for i, (first, last) in enumerate(UNICODE_PAIRS):
        if i < n:
            workers[i].update({
                "First_Name": first, "Last_Name": last,
                "Preferred_Name": f"{first} {last}",
                "Email": f"worker{i}@example.com",
            })
    return _ndjson(workers)


def gen_long_text(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 5))
    length = int(params.get("length", 5000))
    workers = [make_worker(prefix, i) for i in range(n)]
    workers[2]["Bio"] = "x" * length
    return _ndjson(workers)


def gen_empty(params: dict, prefix: str) -> list[str]:
    return []


def gen_all_invalid(params: dict, prefix: str) -> list[str]:
    count = int(params.get("count", 3))
    return ['{"oops": ', "definitely not json", "[1, 2, 3]"][:count]


def gen_null_emails(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 10))
    ratio = float(params.get("null_ratio", 0.6))
    k = int(math.ceil(n * ratio))
    workers = [make_worker(prefix, i) for i in range(n)]
    for i in range(k):
        workers[i]["Email"] = None
    return _ndjson(workers)


def gen_promotion(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 5))
    t = int(params.get("target_index", 0))
    workers = [make_worker(prefix, i) for i in range(n)]
    workers[t]["Job_Profile"] = params.get("new_title", "Senior Engineer")
    workers[t]["Annual_Salary"] = params.get("new_salary", 150000)
    return _ndjson(workers)


def gen_location_change(params: dict, prefix: str) -> list[str]:
    n = int(params.get("n", 5))
    target = int(params["target_index"])
    new_loc = params["new_location"]
    workers = [
        make_worker(prefix, i, Location=new_loc) if i == target else make_worker(prefix, i)
        for i in range(n)
    ]
    return _ndjson(workers)


GENERATORS: dict[str, Callable[[dict, str], list[str]]] = {
    "initial_load": gen_initial_load,
    "dept_change": gen_dept_change,
    "terminate": gen_terminate,
    "rehire": gen_rehire,
    "add_workers": gen_add_workers,
    "with_malformed": gen_with_malformed,
    "missing_worker_id": gen_missing_worker_id,
    "duplicate_rows": gen_duplicate_rows,
    "extra_fields": gen_extra_fields,
    "invalid_dates": gen_invalid_dates,
    "unicode_names": gen_unicode_names,
    "long_text": gen_long_text,
    "empty": gen_empty,
    "all_invalid": gen_all_invalid,
    "null_emails": gen_null_emails,
    "promotion": gen_promotion,
    "location_change": gen_location_change,
}


def generate(name: str, params: dict, prefix: str) -> list[str]:
    try:
        gen = GENERATORS[name]
    except KeyError:
        raise RuntimeError(f"unknown fixture generator {name!r}")
    return gen(params or {}, prefix)
