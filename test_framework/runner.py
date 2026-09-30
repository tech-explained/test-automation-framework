"""QA framework runner: metadata-driven, persistence-driven test orchestration.

Reads test definitions from tf.* tables (never from config files), then for
each case: generate fixture NDJSON -> upload to storage -> ingest via the
pipeline adapter (test_framework/pipeline_adapter.py: your pipeline plugs in
through tf.environments.ingest_adapter) ->
evaluate SQL assertions -> persist every step back to tf.* tables ->
render a Markdown report.

Usage:
    HR_PG_DSN=postgresql://... python -m test_framework.runner --env local --suite smoke
    HR_PG_DSN=postgresql://... python -m test_framework.runner --env local --cases TC-001,TC-003
    HR_PG_DSN=postgresql://... python -m test_framework.runner --env local --suite regression --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
import uuid
from datetime import date

import psycopg
from psycopg.rows import dict_row

from test_framework import assertions as A
from test_framework import batching as B
from test_framework import db as TFDB
from test_framework import fixtures as F
from test_framework import pipeline_adapter as PA
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


def _generate_case_files(case: dict, prefix: str) -> list[dict]:
    """Generate fixture file dicts for one case (no upload, no ingest).

    Returns list of {"fidx", "as_of" (date), "lines", "content", "file_name"}.
    Content format matches the per-file ingest path exactly.
    """
    out = []
    for fidx, fspec in enumerate(case["fixture_sequence"]):
        lines = F.generate(fspec["generator"], fspec.get("params", {}), prefix)
        content = ("\n".join(lines) + "\n") if lines else ""
        out.append({
            "fidx": fidx,
            "as_of": date.fromisoformat(fspec["as_of_date"]),
            "lines": lines,
            "content": content,
            "file_name": f"{case['test_case_id']}-exec-part{fidx}.ndjson",
        })
    return out


def _run_batch_phase1(*, run_id, backend, ingest, env, actor, cases):
    """Batch mode phase 1: generate, merge, upload, ingest.

    ``cases``: list of (case, prefix). Returns
    (preingested, solo_cases, stats) where preingested maps
    (case_id, exec_no, fidx) -> {"uri", "file_id", "status", "error",
    "batch_base"} for batched files, solo_cases is the list of
    (case, prefix, reason) that stay on per-file ingest, and stats
    describes the plan.
    """
    batched_files: list[dict] = []
    solo_cases: list[tuple] = []
    manifest: dict[tuple, str] = {}
    for case, prefix in cases:
        case_id = case["test_case_id"]
        n_exec = int(case.get("executions", 1))
        for exec_no in range(1, n_exec + 1):
            gen = _generate_case_files(case, prefix)
            eligible, reason = B.is_batch_eligible(
                case, [g["content"] for g in gen])
            if not eligible:
                solo_cases.append((case, prefix, reason))
                break
            for g in gen:
                batched_files.append({
                    "case_id": case_id, "exec_no": exec_no, "fidx": g["fidx"],
                    "as_of": g["as_of"], "lines": g["lines"],
                    "content": g["content"]})
    # upload per-case manifest copies (ingested bytes live in merged files;
    # the manifest keeps file_bytes round-trip assertions working per file)
    for bf in batched_files:
        dest = (f"test-artifacts/{run_id}/batch-manifest/{bf['case_id']}/"
                f"exec{bf['exec_no']}/part-{bf['fidx']}.ndjson")
        manifest[(bf["case_id"], bf["exec_no"], bf["fidx"])] = \
            backend.upload_text(dest, bf["content"])

    groups = B.plan_batch_groups(batched_files)
    preingested: dict[tuple, dict] = {}
    group_file_ids: dict[date, dict] = {}
    for grp in groups:
        dest = f"test-artifacts/{run_id}/batch/{grp['as_of']}/merged.ndjson"
        uri = backend.upload_text(dest, grp["content"])
        result = ingest(
            uri,
            file_name=f"batch-{grp['as_of']}.ndjson",
            as_of_date=grp["as_of"],
            actor=actor,
        )
        group_file_ids[grp["as_of"]] = result
        for m in grp["members"]:
            key = (m["case_id"], m["exec_no"], m["fidx"])
            preingested[key] = {
                "uri": manifest[key],
                "file_id": result.get("file_id"),
                "status": result.get("status"),
                "error": result.get("error"),
                "batch_base": grp["offsets"][key],
            }
    stats = {
        "batched_files": len(batched_files),
        "batch_launches": len(groups),
        "solo_cases": sorted({c["test_case_id"] for c, _, _ in solo_cases}),
        "solo_reasons": {c["test_case_id"]: r for c, _, r in solo_cases},
        "group_dates": [g["as_of"].isoformat() for g in groups],
    }
    return preingested, solo_cases, stats


def _run_one_case(conn, *, run_id: uuid.UUID, env: dict, backend,
                  case: dict, prefix: str, dry_run: bool,
                  ingest=None, preingested: dict | None = None) -> dict:
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
                if preingested is not None:
                    # Batch mode: merged file already ingested in phase 1.
                    # uri -> per-case manifest copy (file_bytes round-trip);
                    # file_id -> the shared batch load; batch_base -> this
                    # file's 1-based first line inside the merged file.
                    pre = preingested[(case_id, exec_no, fidx)]
                    uri = pre["uri"]
                    gcs_uris.append(uri)
                    file_contents.append(content)
                    result = {"file_id": pre["file_id"],
                              "status": pre["status"],
                              "error": pre["error"]}
                    if pre.get("batch_base") is not None:
                        ctx[f"batch_line_base_{fidx}"] = pre["batch_base"]
                else:
                    dest = (f"test-artifacts/{run_id}/{case_id}/"
                            f"exec{exec_no}/part-{fidx}.ndjson")
                    uri = backend.upload_text(dest, content)
                    gcs_uris.append(uri)
                    file_contents.append(content)
                    result = ingest(
                        uri,
                        file_name=f"{case_id}-exec{exec_no}-part{fidx}.ndjson",
                        as_of_date=as_of,
                        actor=f"test-framework:{run_id}",
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
    ap.add_argument("--ingest-mode", default="per-file",
                    choices=("per-file", "batch"),
                    help="per-file: one pipeline launch per fixture file; "
                         "batch: merge batch-eligible cases' files by "
                         "as_of_date and ingest each merged file once")
    args = ap.parse_args(argv)

    conn = TFDB.connect()
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
            if args.ingest_mode == "batch":
                planned = []
                for c in cases:
                    gen = _generate_case_files(
                        c, c["test_case_id"].replace("-", ""))
                    ok, reason = B.is_batch_eligible(
                        c, [g["content"] for g in gen])
                    planned.append((c["test_case_id"], ok, reason, gen))
                batched = [p for p in planned if p[1]]
                files = [{"case_id": cid, "exec_no": 1, "fidx": g["fidx"],
                          "as_of": g["as_of"], "lines": g["lines"]}
                         for cid, _, _, gen in batched for g in gen]
                groups = B.plan_batch_groups(files)
                print(f"  batch plan: {len(files)} files -> "
                      f"{len(groups)} launches "
                      f"({', '.join(g['as_of'].isoformat() for g in groups)})")
                for cid, ok, reason, _ in planned:
                    print(f"  {'BATCH' if ok else 'SOLO '} {cid}"
                          + ("" if ok else f" ({reason})"))
                return 0
            for c in cases:
                _run_one_case(conn, run_id=uuid.uuid4(), env=env,
                              backend=None, case=c,
                              prefix=c["test_case_id"].replace("-", ""),
                              dry_run=True)
            return 0

        backend = storage.make_backend(
            env["storage_backend"], bucket=env.get("gcs_bucket"),
            local_root=env.get("local_bucket_root"))
        ingest = PA.load_ingest(env, pipeline)

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
        batch_stats = None
        preingested: dict | None = None
        try:
            cases_with_prefix = [
                (case, f"{case['test_case_id'].replace('-', '')}-{run_short}")
                for case in cases]
            if args.ingest_mode == "batch":
                preingested, solo_cases, batch_stats = _run_batch_phase1(
                    run_id=run_id, backend=backend, ingest=ingest, env=env,
                    actor=f"test-framework:{run_id}",
                    cases=cases_with_prefix)
                solo_ids = {c["test_case_id"] for c, _, _ in solo_cases}
                batched = [(c, p) for c, p in cases_with_prefix
                           if c["test_case_id"] not in solo_ids]
                print(f"Batch ingest: {batch_stats['batched_files']} files -> "
                      f"{batch_stats['batch_launches']} launches "
                      f"({', '.join(batch_stats['group_dates'])}); "
                      f"{len(solo_ids)} solo cases: "
                      f"{', '.join(sorted(solo_ids)) or 'none'}")
                order = [(c, p, True) for c, p in batched] + \
                        [(c, p, False) for c, p, _ in solo_cases]
            else:
                order = [(c, p, False) for c, p in cases_with_prefix]
            for case, prefix, is_batched in order:
                print(f"-> {case['test_case_id']}: {case['name']}"
                      + (" [batch]" if is_batched else ""))
                res = _run_one_case(conn, run_id=run_id, env=env,
                                    backend=backend, case=case,
                                    prefix=prefix, dry_run=False,
                                    ingest=ingest,
                                    preingested=preingested if is_batched else None)
                for k in totals:
                    totals[k] += res.get(k, 0)
        finally:
            _unlock_env(conn, args.env)

        run_status = "passed" if totals["failed"] == 0 and totals["error"] == 0 else "failed"
        summary = {"case_executions": totals, "ingest_mode": args.ingest_mode}
        if batch_stats is not None:
            summary["batch"] = batch_stats
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE tf.test_runs
                      SET status = %s, finished_at = now(),
                          summary = %s::jsonb
                    WHERE run_id = %s""",
                (run_status, json.dumps(summary), run_id))

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
