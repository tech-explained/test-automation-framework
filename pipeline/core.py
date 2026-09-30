"""Shared, Beam-free core logic for the HR Dataflow pipeline.

Everything here is pure Python (no apache_beam, no psycopg imports) so it can
be unit-tested directly and reused by:
  * pipeline/transforms.py  (Beam DoFns for Dataflow)
  * pipeline/launcher.py    (local/direct execution path)
  * test_framework/fixtures.py (fixture generation + validation)

The silver merge itself lives in Postgres (silver.apply_bronze_batch); the
hash canonicalization here is for bronze row identity and for unit-testing
change-detection semantics.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import date, datetime
from typing import Any, Optional

# Canonical home of the NDJSON line semantics is test_framework.lineparse
# (the framework must not import the pipeline package). Re-exported here so
# the bundled reference pipeline shares the single implementation.
from test_framework.lineparse import (
    extract_worker_id,
    parse_line,
    row_hash,
)

# Workday RaaS worker fields we promote to typed silver columns.
# Anything else in the JSON object lands in silver.attributes (schema-drift safe).
KNOWN_FIELDS = [
    "Worker_ID", "Employee_ID", "First_Name", "Last_Name", "Preferred_Name",
    "Email", "Hire_Date", "Termination_Date", "Worker_Status", "Job_Profile",
    "Job_Family", "Department", "Department_ID", "Location", "Country",
    "Manager_Worker_ID", "Cost_Center", "Employment_Type", "Worker_Type",
    "Time_Type", "Compensation_Grade", "Annual_Salary", "Currency",
]

_AS_OF_RE = re.compile(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})")


# parse_line / extract_worker_id / row_hash live in test_framework.lineparse
# and are re-exported above.


def file_id_for_content(sha256_hex: str) -> uuid.UUID:
    """Content-addressed file id: identical bytes => identical id => the
    ingestion registry can never process the same file twice."""
    return uuid.uuid5(uuid.NAMESPACE_URL, f"workday-hr:{sha256_hex}")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_business(obj: dict) -> dict:
    """Canonical business payload used for change detection.

    Mirrors the SQL canonicalization in silver.apply_bronze_batch loosely:
    fixed field order, normalized scalars, unknown fields in `attributes`.
    (The two hashes do not need to be equal; each is consistent within its own
    domain: Python-side for tests, SQL-side for the real merge.)
    """
    fields: dict[str, Any] = {}
    for key in KNOWN_FIELDS:
        if key == "Worker_ID":
            continue
        val = obj.get(key)
        if isinstance(val, str):
            val = val.strip() or None
        fields[key] = val
    attributes = {k: v for k, v in obj.items() if k not in KNOWN_FIELDS}
    return {
        "worker_id": extract_worker_id(obj),
        "fields": fields,
        "attributes": attributes,
    }


def record_hash(obj: dict) -> str:
    """Stable sha256 over the canonical business payload (key order independent)."""
    canon = json.dumps(
        canonical_business(obj),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def business_fields_equal(a: dict, b: dict) -> bool:
    """True when two worker objects carry identical business data."""
    return record_hash(a) == record_hash(b)


def dedupe_keep_last(rows: list[tuple[int, dict]]) -> tuple[list[tuple[int, dict]], int, list[str]]:
    """Dedupe (line_no, obj) rows on worker_id, keeping the LAST line.

    Returns (deduped_rows, duplicate_row_count, [worker_ids with dups]).
    Mirrors the SQL ROW_NUMBER() ... ORDER BY line_no DESC in the merge proc.
    """
    latest: dict[str, tuple[int, dict]] = {}
    dup_ids: set[str] = set()
    dup_count = 0
    for line_no, obj in rows:
        wid = extract_worker_id(obj)
        if wid is None:
            continue
        if wid in latest:
            dup_count += 1
            dup_ids.add(wid)
        latest[wid] = (line_no, obj)
    return list(latest.values()), dup_count, sorted(dup_ids)


def decide_action(
    current: Optional[dict],
    incoming_hash: str,
    file_as_of: date,
    current_as_of: Optional[date] = None,
    current_hash: Optional[str] = None,
) -> str:
    """SCD4 decision for one worker, mirroring silver.apply_bronze_batch.

    Returns one of: 'insert' | 'update' | 'noop' | 'stale'.
      current=None            -> 'insert'   (new worker)
      file_as_of < current_as_of -> 'stale'  (out-of-order file; never regress)
      incoming_hash == current_hash -> 'noop'
      otherwise               -> 'update'   (archive current -> history, new version)
    """
    if current is None:
        return "insert"
    if current_as_of is not None and file_as_of < current_as_of:
        return "stale"
    if current_hash is not None and incoming_hash == current_hash:
        return "noop"
    return "update"


def as_of_from_filename(file_name: str) -> Optional[date]:
    """Best-effort as-of date from names like workers_20260929.ndjson."""
    m = _AS_OF_RE.search(file_name or "")
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def utcnow_iso() -> str:
    return datetime.now().astimezone().isoformat()
