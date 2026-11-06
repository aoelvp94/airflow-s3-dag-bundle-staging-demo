"""Host-side checks for vendor/s3.py (apache/airflow 602dc7b).

Airflow itself is stubbed. The test locks the behavior this demo relies on:
in-tree ETag marker ``{s3_dags_dir}/.archive_etag``, restage when the bundle
directory has no files besides that marker, 1 MiB ``copyfileobj``, and a
single ``tarfile.data_filter`` check in the constructor (extract does not
repeat it). ``S3DagBundle.refresh`` syncs with ``delete_stale`` and does not
unlink the marker itself.
"""

from __future__ import annotations

import importlib.util
import io
import shutil
import sys
import tarfile
import tempfile
import types
import unittest
from contextlib import contextmanager
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
VENDORED = REPO / "vendor" / "s3.py"
COPYFILEOBJ_LENGTH = 1024 * 1024


class _Log:
    def bind(self, **_kwargs):
        return self

    def debug(self, *_args, **_kwargs):
        return None

    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None


class _Body:
    def __init__(self, data: bytes):
        self._bio = io.BytesIO(data)
        self.closed = False

    def read(self, n: int = -1) -> bytes:
        return self._bio.read(n)

    def close(self) -> None:
        self.closed = True


class _Client:
    def __init__(self, payload: bytes, etag: str):
        self.payload = payload
        self.etag = etag
        self.heads = 0
        self.gets: list[dict] = []
        self.bodies: list[_Body] = []

    def head_object(self, **_kwargs):
        self.heads += 1
        return {"ETag": self.etag, "ContentLength": len(self.payload)}

    def get_object(self, **kwargs):
        self.gets.append(kwargs)
        body = _Body(self.payload)
        self.bodies.append(body)
        return {"Body": body, "ETag": self.etag}


class _S3Hook:
    client: _Client | None = None
    sync_calls: list[dict] = []

    def __init__(self, aws_conn_id: str | None = None):
        self.aws_conn_id = aws_conn_id
        self.region_name = "us-east-1"

    def check_for_bucket(self, bucket_name: str) -> bool:
        return True

    def check_for_prefix(self, prefix: str, delimiter: str, bucket_name: str) -> bool:
        return True

    def get_conn(self) -> _Client:
        assert self.client is not None
        return self.client

    def sync_to_local_dir(self, **kwargs) -> None:
        self.sync_calls.append(kwargs)


def _tiny_archive() -> bytes:
    buf = io.BytesIO()
    payload = b"print('staged')\n"
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo(name="demo_dag.py")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    return buf.getvalue()


def _install_stubs(root: Path) -> None:
    airflow = types.ModuleType("airflow")
    dag_processing = types.ModuleType("airflow.dag_processing")
    bundles = types.ModuleType("airflow.dag_processing.bundles")
    base = types.ModuleType("airflow.dag_processing.bundles.base")
    providers = types.ModuleType("airflow.providers")
    amazon = types.ModuleType("airflow.providers.amazon")
    aws = types.ModuleType("airflow.providers.amazon.aws")
    hooks = types.ModuleType("airflow.providers.amazon.aws.hooks")
    base_aws = types.ModuleType("airflow.providers.amazon.aws.hooks.base_aws")
    s3_hook_mod = types.ModuleType("airflow.providers.amazon.aws.hooks.s3")
    common = types.ModuleType("airflow.providers.common")
    compat = types.ModuleType("airflow.providers.common.compat")
    sdk = types.ModuleType("airflow.providers.common.compat.sdk")
    structlog = types.ModuleType("structlog")

    class BaseDagBundle:
        def __init__(
            self,
            *,
            name: str,
            refresh_interval: int = 0,
            version: str | None = None,
            view_url_template: str | None = None,
        ) -> None:
            self.name = name
            self.version = version
            self.refresh_interval = refresh_interval
            self._view_url_template = view_url_template
            self.is_initialized = False
            self.base_dir = root / name
            self.base_dir.mkdir(parents=True, exist_ok=True)

        def initialize(self) -> None:
            self.is_initialized = True

        @contextmanager
        def lock(self):
            yield

    class AwsBaseHook:
        default_conn_name = "aws_default"

    class AirflowException(Exception):
        pass

    base.BaseDagBundle = BaseDagBundle
    base_aws.AwsBaseHook = AwsBaseHook
    s3_hook_mod.S3Hook = _S3Hook
    sdk.AirflowException = AirflowException
    structlog.get_logger = lambda *_a, **_k: _Log()

    for name, module in {
        "airflow": airflow,
        "airflow.dag_processing": dag_processing,
        "airflow.dag_processing.bundles": bundles,
        "airflow.dag_processing.bundles.base": base,
        "airflow.providers": providers,
        "airflow.providers.amazon": amazon,
        "airflow.providers.amazon.aws": aws,
        "airflow.providers.amazon.aws.hooks": hooks,
        "airflow.providers.amazon.aws.hooks.base_aws": base_aws,
        "airflow.providers.amazon.aws.hooks.s3": s3_hook_mod,
        "airflow.providers.common": common,
        "airflow.providers.common.compat": compat,
        "airflow.providers.common.compat.sdk": sdk,
        "structlog": structlog,
    }.items():
        sys.modules[name] = module


def _load_bundle_module():
    spec = importlib.util.spec_from_file_location("vendored_s3_bundle_602dc7b", VENDORED)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {VENDORED}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class VendoredS3BundleTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        _S3Hook.client = _Client(_tiny_archive(), etag='"etag-1"')
        _S3Hook.sync_calls = []
        _install_stubs(self.root)
        self.bundle_mod = _load_bundle_module()
        self.copied_lengths: list[int | None] = []
        real_copy = shutil.copyfileobj

        def _record_copy(src, dst, length=COPYFILEOBJ_LENGTH):
            self.copied_lengths.append(length)
            return real_copy(src, dst, length)

        self.bundle_mod.shutil.copyfileobj = _record_copy

    def tearDown(self) -> None:
        self._tmpdir.cleanup()
        sys.modules.pop("vendored_s3_bundle_602dc7b", None)

    def test_archive_marker_restage_and_copy_size(self) -> None:
        require_calls: list[str] = []
        real_require = self.bundle_mod._require_tar_data_filter

        def _count_require() -> None:
            require_calls.append("require")
            return real_require()

        self.bundle_mod._require_tar_data_filter = _count_require

        bundle = self.bundle_mod.S3ArchiveDagBundle(
            name="bench-archive",
            bucket_name="dag-bundles",
            archive_key="archive/dag-bundle.tar.gz",
            aws_conn_id="aws_default",
        )
        stale = bundle._get_archive_state_dir() / "archive-staging-leftover"
        stale.mkdir(parents=True)
        (stale / "junk").write_text("x")

        bundle.initialize()

        # Constructor checks tarfile.data_filter once. Extract does not.
        self.assertEqual(require_calls, ["require"])
        marker = bundle.path / ".archive_etag"
        self.assertEqual(marker, bundle._get_archive_etag_path())
        self.assertTrue(marker.is_file())
        self.assertEqual(marker.read_text(), '"etag-1"')
        self.assertFalse((self.root / ".bench-archive" / "etag").exists())
        self.assertEqual((bundle.path / "demo_dag.py").read_text(), "print('staged')\n")
        self.assertFalse(stale.exists())
        self.assertEqual(self.copied_lengths, [COPYFILEOBJ_LENGTH])
        self.assertEqual(len(_S3Hook.client.gets), 1)
        self.assertEqual(_S3Hook.client.gets[0]["IfMatch"], '"etag-1"')
        self.assertTrue(_S3Hook.client.bodies[0].closed)
        self.assertTrue(bundle.is_initialized)

        bundle.refresh()
        self.assertEqual(len(_S3Hook.client.gets), 1)
        self.assertEqual(require_calls, ["require"])

        for path in list(bundle.path.rglob("*")):
            if path.is_file() and path != marker:
                path.unlink()
        self.assertTrue(marker.is_file())
        self.assertFalse(bundle._bundle_dir_has_files())
        bundle.refresh()
        self.assertEqual(len(_S3Hook.client.gets), 2)
        self.assertTrue((bundle.path / "demo_dag.py").is_file())
        self.assertEqual((bundle.path / ".archive_etag").read_text(), '"etag-1"')

        shutil.rmtree(bundle.base_dir)
        self.assertFalse((bundle.path / ".archive_etag").exists())
        bundle.initialize()
        self.assertEqual(len(_S3Hook.client.gets), 3)
        self.assertEqual((bundle.path / ".archive_etag").read_text(), '"etag-1"')
        self.assertFalse((self.root / ".bench-archive" / "etag").exists())
        self.assertEqual(require_calls, ["require"])

    def test_prefix_bundle_leaves_in_tree_etag_to_delete_stale(self) -> None:
        bundle = self.bundle_mod.S3DagBundle(
            name="bench-per-object",
            bucket_name="dag-bundles",
            prefix="dags",
            aws_conn_id="aws_default",
        )
        sibling = self.root / ".bench-per-object" / "etag"
        sibling.parent.mkdir(parents=True)
        sibling.write_text('"old-sibling"')
        in_tree = bundle.s3_dags_dir / ".archive_etag"
        in_tree.write_text('"old-in-tree"')

        bundle.refresh()

        # The prefix class does not unlink markers. delete_stale on the hook
        # is what drops an in-tree .archive_etag that the remote prefix lacks.
        self.assertEqual(sibling.read_text(), '"old-sibling"')
        self.assertEqual(in_tree.read_text(), '"old-in-tree"')
        self.assertEqual(len(_S3Hook.sync_calls), 1)
        self.assertEqual(_S3Hook.sync_calls[0]["s3_prefix"], "dags/")
        self.assertTrue(_S3Hook.sync_calls[0]["delete_stale"])
        self.assertEqual(_S3Hook.sync_calls[0]["local_dir"], bundle.s3_dags_dir)

    def test_constructors_take_one_source_each(self) -> None:
        with self.assertRaises(TypeError):
            self.bundle_mod.S3DagBundle(
                name="prefix-only",
                bucket_name="dag-bundles",
                archive_key="archive/dag-bundle.tar.gz",
            )
        with self.assertRaises(TypeError):
            self.bundle_mod.S3ArchiveDagBundle(
                name="archive-only",
                bucket_name="dag-bundles",
                prefix="dags/",
                archive_key="archive/dag-bundle.tar.gz",
            )


if __name__ == "__main__":
    unittest.main()
