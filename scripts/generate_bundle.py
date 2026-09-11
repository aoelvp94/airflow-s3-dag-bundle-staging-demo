#!/usr/bin/env python3
"""
Generate a synthetic-but-realistic DAG bundle into ./bundle_build:

  bundle_build/dags/                 <- files to upload per-object (stock layout)
      demo_dag.py                    <- one real, trivial DAG
      shared/settings_NNN.py         <- N small "project" modules (plain constants)
  bundle_build/dag-bundle.tar.gz     <- the same file set as ONE archive

Real DAG projects are mostly many small support files (config, helpers, SQL),
which is exactly the shape that makes per-object staging slow: total size is
tiny, request count is not. The filler modules deliberately avoid the words
that make the DAG processor try to parse them as DAG files.

Usage: python3 scripts/generate_bundle.py [n_files] [avg_kb]
"""

from __future__ import annotations

import random
import shutil
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "bundle_build"

DEMO_DAG = '''\
"""A trivial DAG proving the bundle staged and parsed correctly."""

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag


@dag(schedule=None, catchup=False, tags=["bundle-demo"])
def bundle_demo():
    PythonOperator(
        task_id="hello",
        python_callable=lambda: print("staged from an S3 bundle"),
    )


bundle_demo()
'''


def filler_module(rng: random.Random, target_bytes: int) -> str:
    lines = [
        '"""Generated support module (simulates project config/helpers)."""',
        "",
    ]
    i = 0
    size = 0
    while size < target_bytes:
        i += 1
        value = rng.randrange(10**9)
        line = f'SETTING_{i:04d} = {{"key": "k{i:04d}", "value": {value}, "enabled": {value % 2 == 0}}}'
        lines.append(line)
        size += len(line) + 1
    return "\n".join(lines) + "\n"


def main() -> None:
    n_files = int(sys.argv[1]) if len(sys.argv) > 1 else 400
    avg_kb = float(sys.argv[2]) if len(sys.argv) > 2 else 8.0
    rng = random.Random(42)

    dags_dir = BUILD / "dags"
    shutil.rmtree(BUILD, ignore_errors=True)
    (dags_dir / "shared").mkdir(parents=True)

    (dags_dir / "demo_dag.py").write_text(DEMO_DAG)

    for i in range(n_files - 1):
        target = max(512, int(rng.gauss(avg_kb * 1024, avg_kb * 256)))
        (dags_dir / "shared" / f"settings_{i:04d}.py").write_text(filler_module(rng, target))

    archive = BUILD / "dag-bundle.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for path in sorted(dags_dir.rglob("*")):
            if path.is_file():
                tar.add(path, arcname=str(path.relative_to(dags_dir)))

    files = list(dags_dir.rglob("*.py"))
    total = sum(f.stat().st_size for f in files)
    print(f"generated {len(files)} files, {total / 1024:.0f} KiB total "
          f"(avg {total / len(files) / 1024:.1f} KiB)")
    print(f"archive: {archive.relative_to(ROOT)} ({archive.stat().st_size / 1024:.0f} KiB)")


if __name__ == "__main__":
    main()
