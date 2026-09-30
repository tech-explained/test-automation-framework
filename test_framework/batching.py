"""Hybrid batch ingest planning for the QA framework.

Per-file ingest (one pipeline launch per fixture file) gives perfect
per-case load isolation but costs one Dataflow job per file (29 launches
for the 20 seeded cases). Batch mode merges batch-eligible cases' fixture
files into one NDJSON per as_of_date and ingests each merged file once.

Why this is safe:
  * Every worker id is namespaced per case (``{TCxxx}-{run8}-W0000``), so
    merged lines can never collide across cases, and every assertion is
    already keyed on ``{prefix}``.
  * Grouping is by as_of_date because the pipeline stamps one as_of_date
    per load; merging across dates would corrupt date semantics.

Why some cases stay solo (per-file):
  * Poison fixtures (malformed / invalid / missing ids): a bad file must
    only ever sink its own load, never a shared batch.
  * The empty-file case: meaningless once merged into a non-empty file.
  * Replay/dedup tests (two identical files in one sequence, or
    executions > 1): identical content must arrive as separate load
    events; merging would destroy the dedup semantics.

Batch contract for assertion authors:
  * ``{prefix}`` — always safe (per-case worker namespace).
  * ``{file_id}`` / ``{file_id_N}`` — resolve to the *shared batch load's*
    file_id. Safe for "load completed" checks; NOT safe for per-case row
    counts — scope those by ``{prefix}`` instead.
"""

from __future__ import annotations

from datetime import date

# Generators whose whole point is a hostile file: never share a batch.
POISON_GENERATORS = frozenset({
    "with_malformed",
    "all_invalid",
    "invalid_dates",
    "missing_worker_id",
    "empty",
})


def is_batch_eligible(case: dict, contents: list[str] | None = None) -> tuple[bool, str]:
    """Decide whether a case may ride the batch. Returns (eligible, reason).

    ``contents`` (optional) is the list of generated fixture byte-strings,
    one per file in fixture_sequence order. When supplied, two identical
    files (a replay/dedup test) force solo mode.
    """
    if not case.get("batchable", True):
        return False, "batchable=FALSE"
    if int(case.get("executions", 1)) != 1:
        return False, "executions>1 needs separate load events"
    seq = case.get("fixture_sequence") or []
    for fspec in seq:
        if fspec.get("generator") in POISON_GENERATORS:
            return False, f"poison generator {fspec['generator']!r}"
    if contents is not None and len(set(contents)) != len(contents):
        return False, "duplicate fixture content (replay/dedup test)"
    return True, ""


def plan_batch_groups(files: list[dict]) -> list[dict]:
    """Group fixture files by as_of_date for merged ingest.

    Each file dict: ``case_id``, ``exec_no``, ``fidx``, ``as_of`` (a date),
    ``lines`` (list of NDJSON strings, no trailing newline).

    Returns groups sorted by as_of_date. Each group::
        {"as_of": date,
         "members": [file dicts sorted by (case_id, exec_no, fidx)],
         "content": merged NDJSON text,
         "offsets": {(case_id, exec_no, fidx): 1-based first line number}}

    The merged content is exactly the concatenation of each member's
    per-file content (``"\\n".join(lines) + "\\n"``), so offsets are exact
    and the pipeline sees a well-formed NDJSON stream.
    """
    by_date: dict[date, list[dict]] = {}
    for f in files:
        by_date.setdefault(f["as_of"], []).append(f)
    groups = []
    for as_of in sorted(by_date):
        members = sorted(by_date[as_of],
                         key=lambda f: (f["case_id"], f["exec_no"], f["fidx"]))
        parts: list[str] = []
        offsets: dict[tuple[str, int, int], int] = {}
        lineno = 1
        for m in members:
            parts.append(("\n".join(m["lines"]) + "\n") if m["lines"] else "")
            offsets[(m["case_id"], m["exec_no"], m["fidx"])] = lineno
            lineno += len(m["lines"])
        groups.append({"as_of": as_of, "members": members,
                       "content": "".join(parts), "offsets": offsets})
    return groups
