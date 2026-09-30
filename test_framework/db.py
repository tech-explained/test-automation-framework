"""Framework-owned Postgres handle.

Replaces the pipeline package's dbio for framework code, so the test
framework runs standalone: ``python3 -m test_framework.runner`` must not
require the ``pipeline`` package unless a built-in (local/dataflow) ingest
mode is used.
"""

from __future__ import annotations

import os

import psycopg


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
