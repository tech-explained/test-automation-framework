"""Report builder: renders a tf.test_runs run as Markdown from the persisted
tables only (the report is a query, not a log scrape)."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row


def build_report(conn, run_id: str) -> tuple[str, str]:
    """Returns (markdown, filename). Writes the file under test_framework/reports/."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM tf.test_runs WHERE run_id = %s", (run_id,))
        run = cur.fetchone()
        cur.execute(
            """SELECT r.*, c.name, c.category
               FROM tf.test_case_results r
               JOIN tf.test_cases c USING (test_case_id)
               WHERE r.run_id = %s ORDER BY c.test_case_id, r.execution_no""",
            (run_id,))
        cases = cur.fetchall()
        cur.execute(
            """SELECT test_case_id, execution_no, name, status, expected, actual, message
               FROM tf.assertion_results
               WHERE run_id = %s AND status <> 'pass'
               ORDER BY test_case_id, execution_no, assertion_id""",
            (run_id,))
        failures = cur.fetchall()

    total = len(cases)
    passed = sum(1 for c in cases if c["status"] == "passed")
    failed = sum(1 for c in cases if c["status"] == "failed")
    errored = sum(1 for c in cases if c["status"] == "error")

    lines = [
        f"# Test run {run_id}",
        "",
        f"- **Suite:** `{run['suite']}` on env `{run['env_id']}`",
        f"- **Status:** {run['status']}",
        f"- **Started:** {run['started_at']}  **Finished:** {run['finished_at']}",
        f"- **Triggered by:** {run['triggered_by']}",
        f"- **Cases:** {total} total — {passed} passed, {failed} failed, {errored} errored",
        "",
        "## Cases",
        "",
        "| Case | Category | Exec | Status | Files ingested | Duration |",
        "| ---- | -------- | ---- | ------ | -------------- | -------- |",
    ]
    for c in cases:
        dur = ""
        if c["started_at"] and c["finished_at"]:
            dur = f"{(c['finished_at'] - c['started_at']).total_seconds():.1f}s"
        files = ", ".join((c["file_ids"] or [])[:2])
        if len(c["file_ids"] or []) > 2:
            files += f" (+{len(c['file_ids']) - 2} more)"
        lines.append(
            f"| {c['test_case_id']} {c['name']} | {c['category']} | "
            f"#{c['execution_no']} | **{c['status']}** | `{files}` | {dur} |"
        )

    if failures:
        lines += ["", "## Failed / errored assertions", ""]
        for f in failures:
            lines += [
                f"### {f['test_case_id']} exec #{f['execution_no']} — `{f['name']}`",
                f"- status: **{f['status']}**",
                f"- expected: `{f['expected']}`",
                f"- actual: `{f['actual']}`",
                f"- message: {f['message']}",
                "",
            ]
    else:
        lines += ["", "All assertions passed. :tada:".replace(":tada:", ""), ""]

    lines += [
        "## Auditability",
        "",
        "Every number above is backed by persisted rows:",
        "- `tf.test_runs` / `tf.test_case_results` / `tf.assertion_results` for the framework trail,",
        "- your pipeline's own ingestion/audit tables for the pipeline trail,",
        "- fixture NDJSON artifacts under the run's storage prefix.",
        "",
        f"_Report generated {datetime.now(timezone.utc).isoformat()}_",
    ]
    md = "\n".join(lines)

    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{run_id}.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(md)
    return md, path
