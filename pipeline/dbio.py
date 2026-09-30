"""Postgres I/O helpers. Thin wrappers around psycopg; no business logic."""

from __future__ import annotations

import json
import os
import uuid
from datetime import date
from typing import Any, Iterable, Optional

import psycopg
from psycopg import sql


def dsn() -> str:
    try:
        return os.environ["HR_PG_DSN"]
    except KeyError:
        raise RuntimeError(
            "HR_PG_DSN is not set. Example: "
            "postgresql://hatch:hatch@127.0.0.1/hrdemo"
        )


def connect() -> psycopg.Connection:
    return psycopg.connect(dsn())


# ---------------------------------------------------------------------------
# bronze writes (idempotent: ON CONFLICT DO NOTHING on (file_id, line_no))
# ---------------------------------------------------------------------------

_BRONZE_COLS = (
    "file_id", "file_name", "line_no", "as_of_date", "worker_json", "row_hash",
)

_BRONZE_INSERT = """
INSERT INTO bronze.raw_worker_events
    (file_id, file_name, line_no, as_of_date, worker_json, row_hash)
VALUES (%(file_id)s, %(file_name)s, %(line_no)s, %(as_of_date)s,
        %(worker_json)s::jsonb, %(row_hash)s)
ON CONFLICT (file_id, line_no) DO NOTHING
"""

_REJECT_INSERT = """
INSERT INTO bronze.raw_worker_rejects
    (file_id, file_name, line_no, raw_line, error)
VALUES (%(file_id)s, %(file_name)s, %(line_no)s, %(raw_line)s, %(error)s)
"""


def write_bronze_batch(conn: psycopg.Connection, rows: Iterable[dict]) -> int:
    """Insert parsed rows; returns number actually inserted (conflicts skipped)."""
    rows = list(rows)
    if not rows:
        return 0
    payload = [
        {
            **r,
            "worker_json": json.dumps(r["worker_json"], ensure_ascii=False),
        }
        for r in rows
    ]
    with conn.cursor() as cur:
        cur.executemany(_BRONZE_INSERT, payload)
        return cur.rowcount if cur.rowcount is not None else 0


def write_rejects_batch(conn: psycopg.Connection, rows: Iterable[dict]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(_REJECT_INSERT, rows)
        return cur.rowcount if cur.rowcount is not None else 0


# ---------------------------------------------------------------------------
# ingestion registry + merge + dq
# ---------------------------------------------------------------------------

def get_ingestion(conn: psycopg.Connection, file_id: uuid.UUID) -> Optional[dict]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT * FROM ops.file_ingestions WHERE file_id = %s", (file_id,)
        )
        return cur.fetchone()


def get_ingestion_by_sha(conn: psycopg.Connection, sha256_hex: str) -> Optional[dict]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT * FROM ops.file_ingestions WHERE file_sha256 = %s",
            (sha256_hex,),
        )
        return cur.fetchone()


def create_ingestion(
    conn: psycopg.Connection,
    *,
    file_id: uuid.UUID,
    file_name: str,
    gcs_uri: str,
    sha256_hex: str,
    as_of_date: date,
    rows_received: int = 0,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.file_ingestions
                (file_id, file_name, gcs_uri, file_sha256, as_of_date,
                 status, rows_received, started_at)
            VALUES (%s, %s, %s, %s, %s, 'processing', %s, now())
            ON CONFLICT (file_id) DO UPDATE SET
                status = 'processing',
                started_at = now(),
                finished_at = NULL,
                error = NULL
            """,
            (file_id, file_name, gcs_uri, sha256_hex, as_of_date, rows_received),
        )


def update_ingestion(conn: psycopg.Connection, file_id: uuid.UUID, **fields: Any) -> None:
    allowed = {
        "status", "rows_received", "rows_loaded", "rows_rejected",
        "workers_upserted", "history_rows_added", "workers_skipped_stale",
        "dq_warnings", "error", "started_at", "finished_at",
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    if "dq_warnings" in updates and not isinstance(updates["dq_warnings"], str):
        updates["dq_warnings"] = json.dumps(updates["dq_warnings"])
    set_clause = sql.SQL(", ").join(
        sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder(k))
        for k in updates
    )
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("UPDATE ops.file_ingestions SET {} WHERE file_id = %(file_id)s").format(
                set_clause
            ),
            {**updates, "file_id": file_id},
        )


def apply_merge(conn: psycopg.Connection, file_id: uuid.UUID) -> dict:
    """Run the SCD4 merge proc; returns dict of counts."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT * FROM silver.apply_bronze_batch(%s)", (file_id,))
        row = cur.fetchone()
        return dict(row) if row else {}


def compute_dq_warnings(conn: psycopg.Connection, file_id: uuid.UUID) -> list:
    with conn.cursor() as cur:
        cur.execute("SELECT ops.compute_dq_warnings(%s)", (file_id,))
        val = cur.fetchone()[0]
    if val is None:
        return []
    return json.loads(val) if isinstance(val, str) else val


def audit(
    conn: psycopg.Connection,
    *,
    actor: str,
    action: str,
    entity: Optional[str] = None,
    entity_id: Optional[str] = None,
    details: Optional[dict] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.audit_log (actor, action, entity, entity_id, details)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            """,
            (actor, action, entity, entity_id,
             json.dumps(details or {}, ensure_ascii=False)),
        )


def create_pipeline_run(
    conn: psycopg.Connection,
    *,
    file_id: uuid.UUID,
    mode: str,
    dataflow_job_id: Optional[str] = None,
    details: Optional[dict] = None,
) -> uuid.UUID:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.pipeline_runs (file_id, mode, dataflow_job_id, status, details)
            VALUES (%s, %s, %s, 'running', %s::jsonb)
            RETURNING run_id
            """,
            (file_id, mode, dataflow_job_id,
             json.dumps(details or {}, ensure_ascii=False)),
        )
        return cur.fetchone()[0]


def finish_pipeline_run(
    conn: psycopg.Connection, run_id: uuid.UUID, status: str,
    details: Optional[dict] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops.pipeline_runs
               SET status = %s, finished_at = now(),
                   details = details || %s::jsonb
             WHERE run_id = %s
            """,
            (status, json.dumps(details or {}, ensure_ascii=False), run_id),
        )
