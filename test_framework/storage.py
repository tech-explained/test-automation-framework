"""Storage backends: the framework writes fixtures through the same abstraction
in every environment. 'local' treats a directory as the bucket (fast,
hermetic, no GCP needed); 'gcs' uses real Google Cloud Storage."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod


class StorageBackend(ABC):
    @abstractmethod
    def upload_text(self, dest_path: str, content: str) -> str:
        """Write text; return the URI the launcher should ingest
        (a local path for LocalStorageBackend, gs://... for GCSBackend)."""

    @abstractmethod
    def read_bytes(self, uri: str) -> bytes:
        """Read back the raw bytes previously written; used by the
        file_bytes assertion kind to verify storage round-trip fidelity."""

    @abstractmethod
    def describe(self) -> str:
        ...


class LocalStorageBackend(StorageBackend):
    def __init__(self, root: str):
        self.root = os.path.abspath(os.path.expanduser(root))
        os.makedirs(self.root, exist_ok=True)

    def upload_text(self, dest_path: str, content: str) -> str:
        full = os.path.join(self.root, dest_path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as fh:
            fh.write(content)
        return full

    def read_bytes(self, uri: str) -> bytes:
        with open(uri, "rb") as fh:
            return fh.read()

    def describe(self) -> str:
        return f"local:{self.root}"


class GCSBackend(StorageBackend):
    def __init__(self, bucket: str):
        from google.cloud import storage  # lazy

        self._client = storage.Client()
        self._bucket = self._client.bucket(bucket)
        self._bucket_name = bucket

    def upload_text(self, dest_path: str, content: str) -> str:
        blob = self._bucket.blob(dest_path)
        blob.upload_from_string(content, content_type="application/x-ndjson")
        return f"gs://{self._bucket_name}/{dest_path}"

    def read_bytes(self, uri: str) -> bytes:
        if not uri.startswith("gs://"):
            raise RuntimeError(f"GCSBackend cannot read non-gs URI {uri!r}")
        path = uri[len("gs://"):]
        bucket_name, _, blob_name = path.partition("/")
        return self._client.bucket(bucket_name).blob(blob_name).download_as_bytes()

    def describe(self) -> str:
        return f"gs://{self._bucket_name}"


def make_backend(kind: str, *, bucket: str | None = None,
                 local_root: str | None = None) -> StorageBackend:
    if kind == "local":
        if not local_root:
            raise RuntimeError("local storage backend needs local_bucket_root")
        return LocalStorageBackend(local_root)
    if kind == "gcs":
        if not bucket:
            raise RuntimeError("gcs storage backend needs gcs_bucket")
        return GCSBackend(bucket)
    raise RuntimeError(f"unknown storage backend {kind!r}")
