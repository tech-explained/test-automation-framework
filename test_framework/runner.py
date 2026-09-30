"""QA framework runner: metadata-driven, persistence-driven test orchestration.

Reads test definitions from tf.* tables (never from config files), then for
each case: generate fixture NDJSON -> upload to storage -> orchestrate the
pipeline via pipeline.launcher.ingest_file -> evaluate SQL assertions ->
persist every step back to tf.* tables -> render a Markdown report.

Usage:
    HR_PG_DSN=postgresql://... python -m test_framework.runner --env local --suite smoke
    HR_PG_DSN=postgresql://... python -m test_framework.runner --env local --cases TC-001,TC-003
    HR_PG_DSN=postgresql://... python -m test_framework.runner --env local --suite regression --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
import uuid
from datetime import date

import psycopg
from psycopg.rows import dict_row

from pipeline import dbio, launcher
from test_framework import assertions as A
from test_framework import fixtures as F
from test_framework import reporting, storage


def _load_env(cur, env_id: str) -> dict:
    cur.execute("SELECT * FROM tf.environments WHERE env_id = %s AND active", (env_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"unknown or inactive environment {env_id!r}")
    return dict(row)


def _load_pipeline(cur) -> dict:
    cur.execute(
        "SELECT * FROM tf.pipelines WHERE active ORDER BY pipeline_id LIMIT 1")
    row = cur.fetchone()
    if not row:
        raise RuntimeError("no active pipeline in tf.pipelines")
    return dict(row)


def _resolve_cases(cur, *, suite=None, cases=None, category=None) -> list[dict]:
    if suite:
        cur.execute("SELECT test_case_ids FROM tf.suites WHERE suite_id = %s AND active",
                    (suite,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"unknown or inactive suite {suite!r}")
        ids = row["test_case_ids"]
    elif cases:
        ids = [c.strip() for c in cases.split(",") if c.strip()]
    elif category:
        cur.execute(
            "SELECT test_case_id FROM tf.test_cases WHERE category = %s AND enabled ORDER BY test_case_id",
            (category,))
        ids = [r["test_case_id"] for r in cur.fetchall()]
    else:
        raise RuntimeError("pass one of --suite, --cases, --category")
    if not ids:
        raise RuntimeError("no test cases selected")
    cur.execute(
        "SELECT * FROM tf.test_cases WHERE test_case_id = ANY(%s) AND enabled ORDER BY test_case_id",
        (ids,))
    found = {r["test_case_id"]: dict(r) for r in cur.fetchall()}
    missing = [i for i in ids if i not in found]
    if missing:
        raise RuntimeError(f"unknown or disabled test cases: {missing}")
    return [found[i] for i in ids]


def _lock_env(conn, env_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (f"tf-run:{env_id}",))


def _unlock_env(conn, env_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (f"tf-run:{env_id}",))


def _dataflow_cfg(env: dict, pipeline: dict) -> dict | None:
    if env["pipeline_mode"] != "dataflow":
        return None
    secret = os.environ.get("DATAFLOW_PG_DSN_SECRET")
    if not secret:
        raise RuntimeError("DATAFLOW_PG_DSN_SECRET must be set for dataflow mode")
    return {
        "project": env["gcp_project"],
        "region": env["dataflow_region"],
        "template_gcs_path": pipeline["flex_template_gcs_path"],
        "pg_dsn_secret": secret,
    }


def _run_one_case(conn, *, run_id: uuid.UUID, env: dict, backend,
                  case: dict, prefix: str, dry_run: bool,
                  dataflow_cfg: dict | None = None) -> dict:
    """Execute all executions of one test case. Returns summary dict."""
    case_id = case["test_case_id"]
    seq = case["fixture_sequence"]
    n_exec = int(case.get("executions", 1))
    overall = {"passed": 0, "failed": 0, "error": 0}

    for exec_no in range(1, n_exec + 1):
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO tf.test_case_results
                       (run_id, test_case_id, execution_no, status)
                   VALUES (%s, %s, %s, 'running')""",
                (run_id, case_id, exec_no))
        conn.commit()
        status = "passed"
        details: dict = {"executions_planned": n_exec}
        file_ids: list[str] = []
        gcs_uris: list[str] = []
        file_contents: list[str] = []
        load_statuses: list[str] = []
        ctx: dict = {"prefix": prefix, "run_id": str(run_id),
                     "test_case_id": case_id}
        try:
            for fidx, fspec in enumerate(seq):
                gen_name = fspec["generator"]
                params = fspec.get("params", {})
                lines = F.generate(gen_name, params, prefix)
                content = ("\n".join(lines) + "\n") if lines else ""
                as_of = date.fromisoformat(fspec["as_of_date"])
                if dry_run:
                    print(f"  [dry-run] {case_id} exec {exec_no}: "
                          f"{gen_name} -> {len(lines)} lines, as_of={as_of}")
                    continue
                dest = (f"test-artifacts/{run_id}/{case_id}/"
                        f"exec{exec_no}/part-{fidx}.ndjson")
                uri = backend.upload_text(dest, content)
                gcs_uris.append(uri)
                file_contents.append(content)
                result = launcher.ingest_file(
                    uri,
                    file_name=f"{case_id}-exec{exec_no}-part{fidx}.ndjson",
                    as_of_date=as_of,
                    mode=env["pipeline_mode"],
                    actor=f"test-framework:{run_id}",
                    dataflow=dataflow_cfg,
                )
                file_ids.append(result["file_id"])
                load_statuses.append(result["status"])
                ctx[f"file_id_{fidx}"] = result["file_id"]
                ctx[f"as_of_{fidx}"] = as_of.isoformat()
                if result["status"] == "failed":
                    raise RuntimeError(
                        f"pipeline ingest failed: {result.get('error')}")
            if not dry_run:
                if file_ids:
                    ctx["file_id"] = file_ids[-1]
                # file kinds (file_bytes, file_rows) evaluate in Python against
                # the exact bytes the runner generated and the backend stored.
                ctx["file_contents"] = file_contents
                ctx["file_uris"] = gcs_uris
                ctx["backend"] = backend
                last_params = seq[-1].get("params", {}) if seq else {}
                ti = last_params.get("target_index", last_params.get("dup_index"))
                if ti is not None:
                    ctx["target_worker"] = f"{prefix}-W{int(ti):04d}"
                assertion_outcomes = []
                for assertion in case["expectations"].get("assertions", []):
                    res = A.run_assertion(conn, assertion, ctx)
                    A.persist_assertion(
                        conn, run_id=run_id, test_case_id=case_id,
                        execution_no=exec_no, result=res)
                    assertion_outcomes.append(res)
                conn.commit()
                failed = [r for r in assertion_outcomes if r["status"] != "pass"]
                status = "passed" if not failed else "failed"
                details["assertions"] = {
                    "total": len(assertion_outcomes),
                    "failed": [r["name"] for r in failed],
                }
        except Exception as exc:  # noqa: BLE001 - per-case isolation
            status = "error"
            details["error"] = f"{type(exc).__name__}: {exc}"
            details["traceback"] = traceback.format_exc(limit=5)
            conn.rollback()
        finally:
            if not dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        """UPDATE tf.test_case_results
                              SET status = %s, finished_at = now(),
                                  gcs_uris = %s::jsonb, file_ids = %s::jsonb,
                                  load_statuses = %s::jsonb,
                                  details = details || %s::jsonb
                            WHERE run_id = %s AND test_case_id = %s
                              AND execution_no = %s""",
                        (status, json.dumps(gcs_uris), json.dumps(file_ids),
                         json.dumps(load_statuses), json.dumps(details),
                         run_id, case_id, exec_no))
                conn.commit()
        overall[status if status in overall else "error"] += 1
        print(f"  [{status.upper()}] {case_id} exec {exec_no} "
              f"({details.get('assertions', {}).get('total', 0)} assertions)")
    return overall


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Metadata-driven QA runner")
    ap.add_argument("--env", default="local")
    ap.add_argument("--suite", default=None)
    ap.add_argument("--cases", default=None, help="comma-separated test case ids")
    ap.add_argument("--category", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--triggered-by", default="qa-framework")
    args = ap.parse_args(argv)

    conn = dbio.connect()
    conn.autocommit = True
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            env = _load_env(cur, args.env)
            pipeline = _load_pipeline(cur)
            suite_label = args.suite or args.cases or args.category or "custom"
            cases = _resolve_cases(cur, suite=args.suite, cases=args.cases,
                                   category=args.category)
        if args.dry_run:
            print(f"DRY RUN on env '{args.env}' ({env['pipeline_mode']}/"
                  f"{env['storage_backend']}): {len(cases)} cases")
            for c in cases:
                _run_one_case(conn, run_id=uuid.uuid4(), env=env,
                              backend=None, case=c,
                              prefix=c["test_case_id"].replace("-", ""),
                              dry_run=True)
            return 0

        backend = storage.make_backend(
            env["storage_backend"], bucket=env.get("gcs_bucket"),
            local_root=env.get("local_bucket_root"))
        df_cfg = _dataflow_cfg(env, pipeline)

        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO tf.test_runs (env_id, suite, triggered_by)
                   VALUES (%s, %s, %s) RETURNING run_id""",
                (args.env, suite_label, args.triggered_by))
            run_id = cur.fetchone()[0]
        print(f"Run {run_id} — env '{args.env}' [{env['pipeline_mode']}/"
              f"{env['storage_backend']}] suite '{suite_label}': "
              f"{len(cases)} cases via {backend.describe()}")

        _lock_env(conn, args.env)
        totals = {"passed": 0, "failed": 0, "error": 0}
        run_short = str(run_id).replace("-", "")[:8]
        try:
            for case in cases:
                prefix = f"{case['test_case_id'].replace('-', '')}-{run_short}"
                print(f"-> {case['test_case_id']}: {case['name']}")
                res = _run_one_case(conn, run_id=run_id, env=env,
                                      backend=backend, case=case,
                                      prefix=prefix, dry_run=False,
                                      dataflow_cfg=df_cfg)
                for k in totals:
                    totals[k] += res.get(k, 0)
        finally:
            _unlock_env(conn, args.env)

        run_status = "passed" if totals["failed"] == 0 and totals["error"] == 0 else "failed"
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE tf.test_runs
                      SET status = %s, finished_at = now(),
                          summary = %s::jsonb
                    WHERE run_id = %s""",
                (run_status, json.dumps({"case_executions": totals}), run_id))

        md, path = reporting.build_report(conn, str(run_id))
        print(f"\nRun {run_id}: {run_status.upper()} "
              f"(passed={totals['passed']} failed={totals['failed']} "
              f"error={totals['error']})")
        print(f"Report: {path}")
        return 0 if run_status == "passed" else 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
