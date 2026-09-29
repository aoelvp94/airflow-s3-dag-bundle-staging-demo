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

``archive_key`` is mutually exclusive with the parent's ``prefix``: the archive
is the only source of the DAGs, so there is no second copy to keep in sync and
no fallback to the per-object sync. An earlier revision of this PoC did fall
back on any archive problem; upstream review concluded that the duality is the
sharp edge of the idea rather than its safety net -- it needs two copies kept
consistent by hand, and it amplifies the failures it is meant to absorb, since
a transient throttle on one GET becomes ~2N requests against the same prefix
from every component staging at that moment.
"""

from __future__ import annotations

import shutil
import tarfile
import tempfile
from pathlib import Path

from airflow.providers.amazon.aws.bundles.s3 import S3DagBundle

ETAG_MARKER = ".staged-archive-etag"


class S3ArchiveDagBundle(S3DagBundle):
    """
    DAG bundle that stages from a single ``.tar.gz`` object in S3.

    :param archive_key: S3 key of the archive (e.g. ``archive/dag-bundle.tar.gz``).
        Members are extracted relative to the bundle root, so pack the contents
        of the DAGs directory rather than the directory itself
        (``tar -C dags -czf dag-bundle.tar.gz .``).
    :param prefix: rejected. The parent uses it as the per-object staging source,
        which this bundle replaces rather than supplements.
    """

    def __init__(self, *, archive_key: str, **kwargs) -> None:
        if kwargs.get("prefix"):
            raise ValueError(
                "S3ArchiveDagBundle takes either 'prefix' or 'archive_key', not both: "
                "the archive is the only staging source."
            )
        super().__init__(**kwargs)
        self.archive_key = archive_key
        self._log = self._log.bind(archive_key=self.archive_key)

    def _initialize(self):
        # The parent validates that `prefix` exists before refreshing. With no
        # prefix and no fallback, the archive is what has to be there.
        with self.lock():
            if not self.s3_dags_dir.exists():
                self.s3_dags_dir.mkdir(parents=True, exist_ok=True)
            if not self.s3_hook.check_for_bucket(bucket_name=self.bucket_name):
                raise RuntimeError(f"S3 bucket '{self.bucket_name}' does not exist.")
            if not self.s3_hook.check_for_key(key=self.archive_key, bucket_name=self.bucket_name):
                raise RuntimeError(
                    f"S3 archive 's3://{self.bucket_name}/{self.archive_key}' does not exist."
                )
            self.refresh()

    def refresh(self) -> None:
        if self.version:
            raise RuntimeError("Refreshing a specific version is not supported")
        with self.lock():
            self._stage_from_archive()

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

            # Atomic-ish swap: rename current out of the way, new one in. The
            # previous tree is parked *inside* the staging dir, so a failed swap
            # can put it back and the cleanup below always removes it.
            backup_dir = staging_dir / "previous"
            if self.s3_dags_dir.exists():
                self.s3_dags_dir.rename(backup_dir)
            try:
                unpack_dir.rename(self.s3_dags_dir)
            except Exception:
                if backup_dir.exists():
                    backup_dir.rename(self.s3_dags_dir)
                raise

            self._log.info(
                "Staged DAG bundle from archive (1 GET instead of per-object sync)",
                archive_bytes=head.get("ContentLength"),
            )
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
