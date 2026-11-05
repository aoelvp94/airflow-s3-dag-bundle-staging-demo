FROM astrocrpublic.azurecr.io/runtime:3.3-7

# Runtime 3.3-7's amazon provider predates apache/airflow#73015. Overlay that
# PR tip (ad0592a555cdc1110e006b5c8b989a5e02e634a6 on
# aoelvp94/airflow@s3-dag-bundle-archive-staging) so the archive classpath is
# airflow.providers.amazon.aws.bundles.s3.S3ArchiveDagBundle (archive_key, no
# prefix) and S3DagBundle stays the per-object prefix sync.
COPY vendor/s3.py /tmp/s3_bundle.py
USER root
RUN python - <<'PY'
import pathlib
import shutil

import airflow.providers.amazon.aws.bundles.s3 as mod

dest = pathlib.Path(mod.__file__)
payload = pathlib.Path("/tmp/s3_bundle.py").read_bytes()
if b"class S3ArchiveDagBundle" not in payload:
    raise SystemExit("vendored s3.py is missing S3ArchiveDagBundle")
dest.write_bytes(payload)
cache = dest.parent / "__pycache__"
if cache.is_dir():
    shutil.rmtree(cache)
pathlib.Path("/tmp/s3_bundle.py").unlink()
print(f"overlaid {dest}")
PY
USER astro
