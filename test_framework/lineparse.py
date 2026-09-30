"""NDJSON line semantics shared by the framework's file_rows verification.

These three pure functions define the bronze contract that the
``file_rows`` assertion kind verifies against:

* a line is *parseable* iff :func:`parse_line` returns ``(obj, None)``
* a parseable line is *accountable* iff :func:`extract_worker_id` is not None
* an accountable line's bronze identity is :func:`row_hash` (sha256 of the
  raw line), matching ``bronze.raw_worker_events.row_hash``

If the pipeline under test uses different bronze identity semantics, write
assertions with the ``sql_scalar`` / ``sql_row`` kinds against its own
tables instead of ``file_rows``.

Canonical home: ``examples/reference_pipeline/core.py`` mirrors these names so
the reference pipeline and the framework share one implementation.
"""

from __future__ import annotations

import hashlib
import json
from typing import Optional


def parse_line(line: str) -> tuple[Optional[dict], Optional[str]]:
    """Parse one NDJSON line. Returns (obj, None) or (None, error_reason)."""
    s = line.strip()
    if not s:
        return None, "empty_line"
    try:
        obj = json.loads(s)
    except json.JSONDecodeError as exc:
        return None, f"invalid_json: {exc.msg}"
    if not isinstance(obj, dict):
        return None, "not_a_json_object"
    return obj, None


def extract_worker_id(obj: dict) -> Optional[str]:
    """Natural key. Returns None when missing/blank (row must be quarantined)."""
    wid = obj.get("Worker_ID")
    if wid is None:
        return None
    wid = str(wid).strip()
    return wid or None


def row_hash(raw_line: str) -> str:
    """Identity hash of the raw line (bronze row identity)."""
    return hashlib.sha256(raw_line.encode("utf-8")).hexdigest()
