"""Orchestrates one file end-to-end: GCS -> (Dataflow | local) -> bronze ->
silver (SCD4 merge proc) -> ingestion registry + audit.

This is the single entry point used by production schedulers AND by the test
framework's "execute" step.

Idempotency contract:
  * file_id is content-addressed (uuid5 of sha256). A byte-identical file that
    already completed is short-circuited as 'skipped_duplicate' before any work.
  * bronze writes are ON CONFLICT DO NOTHING on (file_id, line_no).
  * the silver merge is hash-based: reprocessing identical business data
    creates no new versions and no history rows.
"""

from __future__ import annotations

import functools
import json
import os
import time
import uuid
from datetime import date
from typing import Any, Optional

from examples.reference_pipeline import core, dbio


# ---------------------------------------------------------------------------
# small reliability helpers
# ---------------------------------------------------------------------------

def retry(tries: int = 3, base_delay: float = 2.0, exceptions=(Exception,)):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*a, **kw):
            delay = base_delay
            for attempt in range(1, tries + 1):
                try:
                    return fn(*a, **kw)
                except exceptions as exc:
                    if attempt == tries:
                        raise
                    time.sleep(delay)
                    delay *= 2
            raise AssertionError("unreachable")
        return wrapper
    return deco


@retry(tries=3, base_delay=2.0)
def read_source_bytes(source: str) -> bytes:
    """Read a file from a local path or a gs:// URI."""
    if source.startswith("gs://"):
        from google.cloud import storage  # lazy: only needed for GCS

        path = source[len("gs://"):]
        bucket_name, _, blob_name = path.partition("/")
        client = storage.Client()
        return client.bucket(bucket_name).blob(blob_name).download_as_bytes()
    with open(source, "rb") as fh:
        return fh.read()


def _file_name_of(source: str, override: Optional[str]) -> str:
    if override:
        return override
    return source.rstrip("/").split("/")[-1]


# ---------------------------------------------------------------------------
# bronze load paths
# ---------------------------------------------------------------------------

def _local_bronze_load(
    conn,
    *,
    lines: list[str],
    file_id: uuid.UUID,
    file_name: str,
    as_of: date,
    batch_size: int = 1000,
) -> dict:
    """Pure-Python bronze load (no Beam): identical semantics via pipeline.core."""
    good: list[dict] = []
    bad: list[dict] = []
    loaded = 0
    rejected = 0

    def flush():
        nonlocal loaded, rejected
        if good:
            loaded += dbio.write_bronze_batch(conn, good)
            good.clear()
        if bad:
            rejected += dbio.write_rejects_batch(conn, bad)
            bad.clear()
        conn.commit()

    for i, raw in enumerate(lines, start=1):
        if not raw.strip():
            continue  # ignore blank lines entirely (not even rejects)
        obj, err = core.parse_line(raw)
        if err is not None:
            bad.append({"file_id": file_id, "file_name": file_name, "line_no": i,
                        "raw_line": raw[:8000], "error": err})
        else:
            wid = core.extract_worker_id(obj)
            if wid is None:
                bad.append({"file_id": file_id, "file_name": file_name, "line_no": i,
                            "raw_line": raw[:8000], "error": "missing_worker_id"})
            else:
                good.append({"file_id": file_id, "file_name": file_name, "line_no": i,
                             "as_of_date": as_of, "worker_json": obj,
                             "row_hash": core.row_hash(raw)})
        if len(good) + len(bad) >= batch_size:
            flush()
    flush()
    return {"rows_loaded": loaded, "rows_rejected": rejected}


def _launch_dataflow_job(
    *,
    project: str,
    region: str,
    template_gcs_path: str,
    input_file: str,
    file_id: uuid.UUID,
    file_name: str,
    as_of: date,
    pg_dsn_secret: str,
    job_name: str,
    timeout_s: int = 1800,
    poll_interval_s: int = 15,
) -> str:
    """Launch the Flex Template and block until it reaches a terminal state."""
    from google.cloud.dataflow_v1beta3 import (  # lazy
        FlexTemplatesServiceClient,
        LaunchFlexTemplateParameter,
        LaunchFlexTemplateRequest,
    )

    client = FlexTemplatesServiceClient()
    parent = f"projects/{project}/locations/{region}"
    req = LaunchFlexTemplateRequest(
        project_id=project,
        launch_parameter=LaunchFlexTemplateParameter(
            job_name=job_name,
            container_spec_gcs_path=template_gcs_path,
            parameters={
                "input_file": input_file,
                "file_id": str(file_id),
                "file_name": file_name,
                "as_of_date": as_of.isoformat(),
                "pg_dsn_secret": pg_dsn_secret,
            },
        ),
    )
    resp = client.launch_flex_template(request=req, parent=parent)
    job_id = resp.job.id

    terminal = {"JOB_STATE_DONE", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED",
                "JOB_STATE_DRAINED", "JOB_STATE_UPDATED"}
    deadline = time.time() + timeout_s
    jobs = client  # reuse; get_job via the Jobs service below
    from google.cloud.dataflow_v1beta3 import JobsV1Beta3Client
    jobs_client = JobsV1Beta3Client()
    while time.time() < deadline:
        job = jobs_client.get_job(project_id=project, location=region, job_id=job_id)
        state = job.current_state.name
        if state in terminal:
            if state != "JOB_STATE_DONE":
                raise RuntimeError(f"Dataflow job {job_id} ended in state {state}")
            return job_id
        time.sleep(poll_interval_s)
    raise TimeoutError(f"Dataflow job {job_id} did not finish within {timeout_s}s")


# ---------------------------------------------------------------------------
# main entry point
# ---------------------------------------------------------------------------

def ingest_file(
    source: str,
    *,
    file_name: Optional[str] = None,
    as_of_date: Optional[date] = None,
    mode: str = "local",                      # 'local' | 'dataflow'
    actor: str = "launcher",
    dataflow: Optional[dict] = None,          # project/region/template/... for mode='dataflow'
    batch_size: int = 1000,
) -> dict:
    """Ingest one NDJSON file end-to-end. Returns a result dict (never None).

    Result keys: status ('completed' | 'failed' | 'skipped_duplicate'),
    file_id, file_sha256, rows_received, rows_loaded, rows_rejected,
    workers_upserted, history_rows_added, workers_skipped_stale, dq_warnings,
    dataflow_job_id, error.
    """
    started_wall = time.time()
    fname = _file_name_of(source, file_name)
    as_of = as_of_date or core.as_of_from_filename(fname) or date.today()

    raw_bytes = read_source_bytes(source)
    sha = core.sha256_bytes(raw_bytes)
    file_id = core.file_id_for_content(sha)

    conn = dbio.connect()
    try:
        existing = dbio.get_ingestion_by_sha(conn, sha)
        if existing and existing["status"] == "completed":
            dbio.audit(conn, actor=actor, action="ingest.skipped_duplicate",
                       entity="file", entity_id=str(file_id),
                       details={"file_name": fname, "file_sha256": sha})
            conn.commit()
            return {"status": "skipped_duplicate", "file_id": str(file_id),
                    "file_sha256": sha, "file_name": fname}
        if existing and existing["status"] == "processing":
            raise RuntimeError(
                f"file {fname} (sha {sha[:12]}...) is already marked 'processing'; "
                "refusing concurrent ingest (idempotency guard)")

        text = raw_bytes.decode("utf-8", errors="replace")
        lines = text.splitlines()
        rows_received = sum(1 for ln in lines if ln.strip())

        dbio.create_ingestion(conn, file_id=file_id, file_name=fname,
                              gcs_uri=source, sha256_hex=sha,
                              as_of_date=as_of, rows_received=rows_received)
        dbio.audit(conn, actor=actor, action="ingest.started", entity="file",
                   entity_id=str(file_id),
                   details={"file_name": fname, "gcs_uri": source,
                            "file_sha256": sha, "as_of_date": as_of.isoformat(),
                            "rows_received": rows_received, "mode": mode})
        run_id = dbio.create_pipeline_run(conn, file_id=file_id, mode=mode,
                                          details={"file_name": fname})
        conn.commit()

        dataflow_job_id: Optional[str] = None
        try:
            if mode == "dataflow":
                if not dataflow:
                    raise RuntimeError("mode='dataflow' needs the `dataflow` config dict")
                if not source.startswith("gs://"):
                    raise RuntimeError("dataflow mode needs a gs:// source")
                dataflow_job_id = _launch_dataflow_job(
                    input_file=source, file_id=file_id, file_name=fname,
                    as_of=as_of, job_name=f"hr-bronze-{file_id.hex[:12]}",
                    **dataflow)
                dbio.finish_pipeline_run(conn, run_id, "completed",
                                         {"dataflow_job_id": dataflow_job_id})
            elif mode == "local":
                load_stats = _local_bronze_load(
                    conn, lines=lines, file_id=file_id, file_name=fname,
                    as_of=as_of, batch_size=batch_size)
                dbio.finish_pipeline_run(conn, run_id, "completed", load_stats)
            else:
                raise RuntimeError(f"unknown mode {mode!r}")
            conn.commit()

            # silver SCD4 merge (single transaction inside the proc)
            merge_stats = dbio.apply_merge(conn, file_id)
            dq_warnings = dbio.compute_dq_warnings(conn, file_id)
            dbio.update_ingestion(
                conn, file_id,
                status="completed",
                rows_loaded=_bronze_count(conn, file_id),
                rows_rejected=_reject_count(conn, file_id),
                workers_upserted=merge_stats.get("workers_upserted", 0),
                history_rows_added=merge_stats.get("history_rows_added", 0),
                workers_skipped_stale=merge_stats.get("workers_skipped_stale", 0),
                dq_warnings=dq_warnings,
                finished_at=_now(),
            )
            dbio.audit(conn, actor=actor, action="ingest.completed", entity="file",
                       entity_id=str(file_id),
                       details={"file_name": fname, "merge": merge_stats,
                                "dq_warnings": dq_warnings,
                                "elapsed_s": round(time.time() - started_wall, 2)})
            conn.commit()
            return {"status": "completed", "file_id": str(file_id),
                    "file_sha256": sha, "file_name": fname,
                    "rows_received": rows_received,
                    "rows_loaded": _bronze_count(conn, file_id),
                    "rows_rejected": _reject_count(conn, file_id),
                    "workers_upserted": merge_stats.get("workers_upserted", 0),
                    "history_rows_added": merge_stats.get("history_rows_added", 0),
                    "workers_skipped_stale": merge_stats.get("workers_skipped_stale", 0),
                    "dq_warnings": dq_warnings,
                    "dataflow_job_id": dataflow_job_id, "error": None}
        except Exception as exc:  # noqa: BLE001 - must record any failure
            conn.rollback()
            dbio.update_ingestion(conn, file_id, status="failed",
                                  error=f"{type(exc).__name__}: {exc}",
                                  finished_at=_now())
            dbio.audit(conn, actor=actor, action="ingest.failed", entity="file",
                       entity_id=str(file_id),
                       details={"file_name": fname,
                                "error": f"{type(exc).__name__}: {exc}"})
            dbio.finish_pipeline_run(conn, run_id, "failed",
                                     {"error": f"{type(exc).__name__}: {exc}"})
            conn.commit()
            return {"status": "failed", "file_id": str(file_id),
                    "file_sha256": sha, "file_name": fname,
                    "error": f"{type(exc).__name__}: {exc}"}
    finally:
        conn.close()


def _bronze_count(conn, file_id: uuid.UUID) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM bronze.raw_worker_events WHERE file_id = %s",
            (file_id,))
        return cur.fetchone()[0]


def _reject_count(conn, file_id: uuid.UUID) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM bronze.raw_worker_rejects WHERE file_id = %s",
            (file_id,))
        return cur.fetchone()[0]


def _now():
    from datetime import datetime, timezone  # noqa: PLC0415
    return datetime.now(timezone.utc)


def main(argv=None):
    import argparse  # noqa: PLC0415

    ap = argparse.ArgumentParser(description="Ingest one Workday NDJSON file")
    ap.add_argument("source", help="local path or gs:// URI of the .ndjson file")
    ap.add_argument("--file-name", default=None)
    ap.add_argument("--as-of-date", default=None, help="YYYY-MM-DD")
    ap.add_argument("--mode", default="local", choices=["local", "dataflow"])
    ap.add_argument("--actor", default="launcher")
    args = ap.parse_args(argv)

    as_of = date.fromisoformat(args.as_of_date) if args.as_of_date else None
    result = ingest_file(args.source, file_name=args.file_name,
                         as_of_date=as_of, mode=args.mode, actor=args.actor)
    print(json.dumps(result, indent=2, default=str))
    if result["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
