"""
S3ArchiveDagBundle: stage an Airflow DAG bundle from ONE compressed archive
object instead of downloading the bucket prefix one object at a time.

The stock ``S3DagBundle`` (apache-airflow-providers-amazon) refreshes with
``S3Hook.sync_to_local_dir()``, a serial per-object loop. For a bundle of N
small files that costs ~N GET (+N HEAD) requests and N sequential round-trips
on every fresh worker — which under KubernetesExecutor means on every task
start.

This subclass changes only the staging strategy:

    GET  <archive_key>          (one object, conditional on ETag)
    verify + unpack to a temp dir (path-traversal safe)
    atomic rename swap into place

Any archive problem (missing object, corrupt tar, unpack error) falls back to
the inherited per-object sync, so the worst case is exactly the stock
behaviour.
"""

from __future__ import annotations

import shutil
import tarfile
import tempfile
import uuid
from pathlib import Path

from airflow.providers.amazon.aws.bundles.s3 import S3DagBundle

ETAG_MARKER = ".staged-archive-etag"


class S3ArchiveDagBundle(S3DagBundle):
    """
    DAG bundle that stages from a single ``.tar.gz`` object in S3.

    :param archive_key: S3 key of the archive (e.g. ``archive/dag-bundle.tar.gz``).
        The archive members must be laid out exactly as the files under
        ``prefix`` would be, so the two staging strategies are interchangeable.
    :param prefix: kept from the parent class — used only by the fallback
        per-object sync.
    """

    def __init__(self, *, archive_key: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.archive_key = archive_key
        self._log = self._log.bind(archive_key=self.archive_key)

    def _initialize(self):
        # The parent validates that `prefix` exists before refreshing; the
        # archive path only needs the bucket, so validate that and refresh.
        with self.lock():
            if not self.s3_dags_dir.exists():
                self.s3_dags_dir.mkdir(parents=True, exist_ok=True)
            if not self.s3_hook.check_for_bucket(bucket_name=self.bucket_name):
                raise RuntimeError(f"S3 bucket '{self.bucket_name}' does not exist.")
            self.refresh()

    def refresh(self) -> None:
        if self.version:
            raise RuntimeError("Refreshing a specific version is not supported")
        with self.lock():
            try:
                self._stage_from_archive()
            except Exception:
                self._log.warning(
                    "Staging from archive failed, falling back to per-object sync",
                    exc_info=True,
                )
                super().refresh()

    # -- archive staging ---------------------------------------------------

    def _stage_from_archive(self) -> None:
        client = self.s3_hook.get_conn()

        head = client.head_object(Bucket=self.bucket_name, Key=self.archive_key)
        etag = head.get("ETag", "")

        marker = self.s3_dags_dir / ETAG_MARKER
        if etag and marker.exists() and marker.read_text() == etag:
            self._log.debug("Archive unchanged (ETag match), skipping staging")
            return

        parent = self.s3_dags_dir.parent
        staging_dir = Path(tempfile.mkdtemp(dir=parent, prefix=".archive-staging-"))
        try:
            archive_path = staging_dir / "_bundle_archive"
            client.download_file(self.bucket_name, self.archive_key, str(archive_path))

            unpack_dir = staging_dir / "unpacked"
            unpack_dir.mkdir()
            with tarfile.open(archive_path, "r:*") as tar:
                tar.extractall(unpack_dir, filter="data")
            archive_path.unlink()

            if etag:
                (unpack_dir / ETAG_MARKER).write_text(etag)

            # Atomic-ish swap: rename current out of the way, new one in.
            old_dir = parent / f".archive-old-{uuid.uuid4().hex}"
            if self.s3_dags_dir.exists():
                self.s3_dags_dir.rename(old_dir)
            unpack_dir.rename(self.s3_dags_dir)
            shutil.rmtree(old_dir, ignore_errors=True)

            self._log.info(
                "Staged DAG bundle from archive (1 GET instead of per-object sync)",
                archive_bytes=head.get("ContentLength"),
            )
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
