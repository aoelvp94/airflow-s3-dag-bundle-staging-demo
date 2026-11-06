FROM astrocrpublic.azurecr.io/runtime:3.3-7

# Astro Runtime 3.3-7 does not include the Amazon provider. 9.37.0 is the
# newest release whose apache-airflow-providers-common-compat floor (1.18.0)
# matches this image. After install, replace the provider's S3 bundle module
# with the apache/airflow PR #73015 tip (602dc7b), vendored at vendor/s3.py.
#
# Classpaths the demo imports:
#   airflow.providers.amazon.aws.bundles.s3.S3DagBundle
#   airflow.providers.amazon.aws.bundles.s3.S3ArchiveDagBundle
COPY --chown=astro:0 vendor/s3.py /tmp/s3_bundle_602dc7b.py
RUN pip install --no-cache-dir "apache-airflow-providers-amazon==9.37.0" \
    && python - <<'PY'
from pathlib import Path

import airflow.providers.amazon.aws.bundles.s3 as installed

dest = Path(installed.__file__)
dest.write_bytes(Path("/tmp/s3_bundle_602dc7b.py").read_bytes())
cache = dest.parent / "__pycache__"
if cache.is_dir():
    for compiled in cache.glob("s3*.pyc"):
        compiled.unlink()
print(f"overlaid {dest}")
PY
RUN python -c "from airflow.providers.amazon.aws.bundles.s3 import S3ArchiveDagBundle, S3DagBundle; assert 'archive_key' in S3ArchiveDagBundle.__init__.__annotations__"
