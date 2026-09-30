"""Apache Beam transforms: NDJSON text -> bronze Postgres tables.

Only imported when running under Beam (Dataflow or DirectRunner).
All parsing/validation logic delegates to pipeline.core so the local
execution path behaves identically.
"""

from __future__ import annotations

import json
import uuid
from datetime import date

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.coders import VarIntCoder

from pipeline import core


# ---------------------------------------------------------------------------
# 1-based line numbers. Single-key stateful DoFn: every line of one file flows
# through one worker for numbering, then fans back out. Fine for HR-extract
# scale (tens of thousands of lines); document if files ever reach 100M+ lines.
# ---------------------------------------------------------------------------
class AssignLineNumbers(beam.DoFn):
    SEQ = ReadModifyWriteStateSpec("seq", VarIntCoder())

    def process(self, element, seq=beam.DoFn.StateParam(SEQ)):
        # element: (file_id, file_name, as_of_date, raw_line)
        n = seq.read() or 0
        seq.write(n + 1)
        file_id, file_name, as_of_date, raw_line = element
        yield {
            "file_id": file_id,
            "file_name": file_name,
            "as_of_date": as_of_date,
            "line_no": n + 1,
            "raw_line": raw_line,
        }


# ---------------------------------------------------------------------------
# Parse + validate. Good rows -> main output, bad rows -> "rejects" side output.
# ---------------------------------------------------------------------------
class ParseWorkerLine(beam.DoFn):
    def process(self, element):
        obj, err = core.parse_line(element["raw_line"])
        base = {
            "file_id": element["file_id"],
            "file_name": element["file_name"],
            "line_no": element["line_no"],
        }
        if err is not None:
            yield pvalue.TaggedOutput(
                "rejects",
                {**base,
                 "raw_line": element["raw_line"][:8000],
                 "error": err},
            )
            return
        wid = core.extract_worker_id(obj)
        if wid is None:
            yield pvalue.TaggedOutput(
                "rejects",
                {**base,
                 "raw_line": element["raw_line"][:8000],
                 "error": "missing_worker_id"},
            )
            return
        yield {
            **base,
            "as_of_date": element["as_of_date"],
            "worker_json": obj,
            "row_hash": core.row_hash(element["raw_line"]),
        }


# ---------------------------------------------------------------------------
# Batched bronze writer. Idempotent via ON CONFLICT (file_id, line_no) DO NOTHING.
# ---------------------------------------------------------------------------
class _BronzeWriterBase(beam.DoFn):
    def __init__(self, pg_dsn=None, pg_dsn_secret=None, batch_size=1000):
        self._pg_dsn = pg_dsn
        self._pg_dsn_secret = pg_dsn_secret
        self._batch_size = batch_size

    def setup(self):
        import psycopg  # noqa: PLC0415 - heavy import only on workers

        dsn = self._pg_dsn
        if not dsn and self._pg_dsn_secret:
            from google.cloud import secretmanager  # noqa: PLC0415

            client = secretmanager.SecretManagerServiceClient()
            dsn = client.access_secret_version(
                name=self._pg_dsn_secret
            ).payload.data.decode("utf-8")
        if not dsn:
            raise RuntimeError("No Postgres DSN: pass --pg_dsn or --pg_dsn_secret")
        self._conn = psycopg.connect(dsn)
        self._conn.autocommit = False
        self._buf: list[dict] = []

    def _flush(self):
        if not self._buf:
            return
        from pipeline import dbio  # noqa: PLC0415

        try:
            n = self._write_batch(self._conn, self._buf)
            self._conn.commit()
            self._flush_metric(n)
        except Exception:
            self._conn.rollback()
            raise
        finally:
            self._buf = []

    def _write_batch(self, conn, buf):  # overridden
        raise NotImplementedError

    def _flush_metric(self, n):  # overridden
        pass

    def process(self, element):
        self._buf.append(element)
        if len(self._buf) >= self._batch_size:
            self._flush()

    def finish_bundle(self):
        self._flush()

    def teardown(self):
        try:
            self._flush()
            self._conn.commit()
        finally:
            self._conn.close()


class WriteBronzeEvents(_BronzeWriterBase):
    def _write_batch(self, conn, buf):
        from pipeline import dbio  # noqa: PLC0415

        return dbio.write_bronze_batch(conn, buf)

    def _flush_metric(self, n):
        beam.metrics.Metrics.counter("bronze", "rows_loaded").inc(n)


class WriteBronzeRejects(_BronzeWriterBase):
    def _write_batch(self, conn, buf):
        from pipeline import dbio  # noqa: PLC0415

        return dbio.write_rejects_batch(conn, buf)

    def _flush_metric(self, n):
        beam.metrics.Metrics.counter("bronze", "rows_rejected").inc(n)


def build_bronze_load(
    p: beam.Pipeline,
    *,
    input_file: str,
    file_id: uuid.UUID,
    file_name: str,
    as_of_date: date,
    pg_dsn: str | None = None,
    pg_dsn_secret: str | None = None,
    batch_size: int = 1000,
):
    """PCollection graph: GCS NDJSON -> bronze.raw_worker_events (+ rejects)."""
    lines = (
        p
        | "ReadNDJSON" >> beam.io.ReadFromText(input_file)
        | "AttachFileMeta"
        >> beam.Map(lambda ln: (str(file_id), file_name, as_of_date.isoformat(), ln))
        | "KeyForNumbering" >> beam.Map(lambda t: (1, t))
        | "AssignLineNumbers" >> beam.ParDo(AssignLineNumbers())
    )
    # drop the constant key, keep the dict
    numbered = lines | "Unkey" >> beam.Map(lambda kv: kv[1])

    parsed = numbered | "ParseWorkerLine" >> beam.ParDo(ParseWorkerLine()).with_outputs(
        "rejects", main="events"
    )

    _ = (
        parsed.events
        | "WriteBronzeEvents"
        >> beam.ParDo(
            WriteBronzeEvents(
                pg_dsn=pg_dsn, pg_dsn_secret=pg_dsn_secret, batch_size=batch_size
            )
        )
    )
    _ = (
        parsed.rejects
        | "WriteBronzeRejects"
        >> beam.ParDo(
            WriteBronzeRejects(
                pg_dsn=pg_dsn, pg_dsn_secret=pg_dsn_secret, batch_size=batch_size
            )
        )
    )
    return p


def parse_args(argv=None):
    import argparse  # noqa: PLC0415

    parser = argparse.ArgumentParser(description="Workday HR NDJSON -> bronze loader")
    parser.add_argument("--input_file", required=True, help="gs://.../*.ndjson (or local path for DirectRunner)")
    parser.add_argument("--file_id", required=True, help="content-addressed UUID for this file")
    parser.add_argument("--file_name", required=True)
    parser.add_argument("--as_of_date", required=True, help="YYYY-MM-DD business as-of date")
    parser.add_argument("--pg_dsn", default=None)
    parser.add_argument("--pg_dsn_secret", default=None,
                        help="Secret Manager resource name holding the Postgres DSN")
    parser.add_argument("--batch_size", type=int, default=1000)
    return parser.parse_known_args(argv)
