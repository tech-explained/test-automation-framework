"""Add a QA test case without writing SQL.

The painful way: hand-write an INSERT with JSON-in-SQL quoting.
This way: write a plain JSON spec, run one command.

    python3 -m test_framework.add_case --spec fixtures/samples/tc-021-promotion.json
    python3 -m test_framework.add_case --spec fixtures/samples/tc-021-promotion.json --apply

Without --apply it validates the spec and prints what would happen (dry run).
With --apply it upserts tf.test_cases and appends the case to the suites.

Spec format:
{
  "test_case_id": "TC-021",
  "name": "Promotion changes title and salary",
  "category": "scd4",
  "description": "...",
  "executions": 1,
  "pipeline_id": "my-pipeline-v1",
  "fixture_sequence": [
    {"generator": "initial_load", "params": {"n": 5}, "as_of_date": "2026-09-29"},
    {"generator": "promotion", "params": {"n": 5, "target_index": 0}, "as_of_date": "2026-09-30"}
  ],
  "assertions": [
    {"name": "version_2", "kind": "sql_scalar",
     "sql": "SELECT version FROM myapp.workers_current WHERE worker_id = {target_worker}",
     "op": "eq", "expected": 2}
  ],
  "suites": ["regression", "scd4"]
}

Assertion SQL may use {placeholders}: {prefix}, {file_id}, {file_id_0..N},
{as_of_0..N}, {target_worker}. The runner binds them as query parameters.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import psycopg
from psycopg.types.json import Jsonb

from . import fixtures

CATEGORIES = {"functional", "idempotency", "scd4", "negative", "edge", "audit", "dq"}
KINDS = {"sql_scalar", "sql_row", "file_bytes", "file_rows"}
SQL_KINDS = {"sql_scalar", "sql_row"}
FILE_KINDS = {"file_bytes", "file_rows"}
OPS = {"eq", "ne", "gt", "gte", "lt", "lte"}
# placeholders the runner can bind; {file_id_N}/{as_of_N} checked by pattern
KNOWN_PLACEHOLDERS = {"prefix", "file_id", "target_worker"}


def _dsn() -> str:
    dsn = os.environ.get("HR_PG_DSN")
    if not dsn:
        raise SystemExit("HR_PG_DSN is not set")
    return dsn


def validate(spec: dict) -> list[str]:
    """Return a list of problems (empty = valid)."""
    problems: list[str] = []
    tc = spec.get("test_case_id", "")
    if not re.fullmatch(r"TC-\d{3}", tc or ""):
        problems.append(f"test_case_id {tc!r} must look like 'TC-021'")
    for f in ("name", "category", "description"):
        if not spec.get(f):
            problems.append(f"missing required field: {f}")
    if spec.get("category") not in CATEGORIES:
        problems.append(f"category {spec.get('category')!r} not in {sorted(CATEGORIES)}")
    seq = spec.get("fixture_sequence")
    if not isinstance(seq, list) or not seq:
        problems.append("fixture_sequence must be a non-empty list")
    else:
        for i, step in enumerate(seq):
            gen = (step or {}).get("generator")
            if gen not in fixtures.GENERATORS:
                problems.append(
                    f"fixture_sequence[{i}].generator {gen!r} unknown; "
                    f"known: {sorted(fixtures.GENERATORS)}")
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str((step or {}).get("as_of_date", ""))):
                problems.append(f"fixture_sequence[{i}].as_of_date must be YYYY-MM-DD")
    assertions = spec.get("assertions")
    if not isinstance(assertions, list) or not assertions:
        problems.append("assertions must be a non-empty list")
    else:
        for i, a in enumerate(assertions):
            a = a or {}
            for f in ("name", "kind", "op", "expected"):
                if f not in a:
                    problems.append(f"assertions[{i}] missing field: {f}")
            kind = a.get("kind")
            if kind not in KINDS:
                problems.append(f"assertions[{i}].kind {kind!r} not in {sorted(KINDS)}")
            if a.get("op") not in OPS:
                problems.append(f"assertions[{i}].op {a.get('op')!r} not in {sorted(OPS)}")
            if kind in SQL_KINDS:
                if "sql" not in a:
                    problems.append(f"assertions[{i}] kind {kind!r} needs 'sql'")
                for ph in re.findall(r"\{(\w+)\}", a.get("sql", "")):
                    if (ph not in KNOWN_PLACEHOLDERS
                            and not re.fullmatch(r"(file_id|as_of)_\d+", ph)):
                        problems.append(
                            f"assertions[{i}] uses unknown placeholder {{{ph}}}; "
                            f"known: {sorted(KNOWN_PLACEHOLDERS)} + file_id_N / as_of_N")
            elif kind in FILE_KINDS:
                if "sql" in a:
                    problems.append(
                        f"assertions[{i}] kind {kind!r} takes 'file', not 'sql'")
                ref = str(a.get("file", ""))
                m = re.fullmatch(r"file_(\d+)", ref)
                if not m:
                    problems.append(
                        f"assertions[{i}].file must look like 'file_0', got {ref!r}")
                elif isinstance(seq, list) and int(m.group(1)) >= len(seq):
                    problems.append(
                        f"assertions[{i}].file {ref!r} out of range "
                        f"({len(seq)} fixture files)")
                if kind == "file_bytes" and not isinstance(a.get("expected"), bool):
                    problems.append(
                        f"assertions[{i}] kind 'file_bytes' expects boolean 'expected'")
    return problems


def plan(spec: dict) -> str:
    n_fix = len(spec.get("fixture_sequence", []))
    n_assert = len(spec.get("assertions", []))
    suites = spec.get("suites", [])
    lines = [
        f"test case {spec['test_case_id']}: {spec['name']} [{spec['category']}]",
        f"  fixtures:   {n_fix} file(s) -> "
        + ", ".join(s["generator"] for s in spec["fixture_sequence"]),
        f"  assertions: {n_assert}",
        f"  suites:     {', '.join(suites) if suites else '(none)'}",
        f"  executions: {spec.get('executions', 1)}",
        f"  batchable:  {spec.get('batchable', True)}",
    ]
    return "\n".join(lines)


def apply(spec: dict) -> None:
    tc = spec["test_case_id"]
    batchable = bool(spec.get("batchable", True))
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tf.test_cases
                (test_case_id, name, category, description, executions,
                 fixture_sequence, pipeline_id, expectations, enabled,
                 batchable)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s)
            ON CONFLICT (test_case_id) DO UPDATE SET
                name = EXCLUDED.name, category = EXCLUDED.category,
                description = EXCLUDED.description,
                executions = EXCLUDED.executions,
                fixture_sequence = EXCLUDED.fixture_sequence,
                pipeline_id = EXCLUDED.pipeline_id,
                expectations = EXCLUDED.expectations,
                enabled = TRUE,
                batchable = EXCLUDED.batchable
            """,
            (tc, spec["name"], spec["category"], spec["description"],
             int(spec.get("executions", 1)),
             Jsonb(spec["fixture_sequence"]),
             spec.get("pipeline_id"),
             Jsonb({"assertions": spec["assertions"]}), batchable),
        )
        for suite in spec.get("suites", []):
            cur.execute("SELECT 1 FROM tf.suites WHERE suite_id = %s", (suite,))
            if not cur.fetchone():
                raise SystemExit(f"suite {suite!r} does not exist in tf.suites")
            cur.execute(
                """UPDATE tf.suites SET test_case_ids = test_case_ids || %s::jsonb
                   WHERE suite_id = %s
                     AND NOT (test_case_ids @> %s::jsonb)""",
                (json.dumps([tc]), suite, json.dumps([tc])),
            )
        conn.commit()
    print(f"upserted {tc} and linked suites: {', '.join(spec.get('suites', [])) or '(none)'}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Add a QA test case from a JSON spec (no SQL).")
    ap.add_argument("--spec", required=True, help="path to the JSON spec file")
    ap.add_argument("--apply", action="store_true",
                    help="write to the database (default is validate-only dry run)")
    args = ap.parse_args(argv)

    with open(args.spec) as fh:
        spec = json.load(fh)
    problems = validate(spec)
    if problems:
        print("spec INVALID:")
        for p in problems:
            print(f"  - {p}")
        return 1
    print("spec valid.")
    print(plan(spec))
    if not args.apply:
        print("\ndry run: pass --apply to write to tf.test_cases.")
        return 0
    apply(spec)
    return 0


if __name__ == "__main__":
    sys.exit(main())
