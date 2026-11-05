#!/usr/bin/env python3
"""
Benchmark cold DAG-bundle staging: stock per-object sync vs single archive.

Runs INSIDE an Airflow container (see `just bench`). Each round wipes the
bundle's local directory first, simulating what every fresh worker pod
experiences under KubernetesExecutor: an empty filesystem, so the full
staging cost sits on the critical path of the task start.

S3 request counts are read from MinIO's Prometheus endpoint before/after each
round, so the numbers are what the "S3" server actually served, not estimates.
"""

from __future__ import annotations

import re
import shutil
import statistics
import sys
import time
import urllib.request

from airflow.providers.amazon.aws.bundles.s3 import S3ArchiveDagBundle, S3DagBundle

METRICS_URL = "http://minio:9000/minio/v2/metrics/cluster"
BUCKET = "dag-bundles"
PREFIX = "dags/"
ARCHIVE_KEY = "archive/dag-bundle.tar.gz"
ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 3

INTERESTING_APIS = ("getobject", "headobject", "listobjects")


def s3_request_counts() -> dict[str, int] | None:
    """Sum minio_s3_requests_total by API name; None if metrics unavailable."""
    try:
        text = urllib.request.urlopen(METRICS_URL, timeout=5).read().decode()
    except Exception:
        return None
    counts: dict[str, int] = {}
    for line in text.splitlines():
        if "s3_requests_total{" not in line or line.startswith("#"):
            continue
        api = re.search(r'api="([^"]+)"', line)
        if not api:
            continue
        name = api.group(1).lower()
        counts[name] = counts.get(name, 0) + int(float(line.rsplit(None, 1)[-1]))
    return counts


def diff(before: dict | None, after: dict | None) -> dict[str, int]:
    if before is None or after is None:
        return {}
    return {
        api: after.get(api, 0) - before.get(api, 0)
        for api in set(after) | set(before)
        if after.get(api, 0) != before.get(api, 0)
    }


def run_cold(make_bundle) -> tuple[float, dict[str, int], int]:
    bundle = make_bundle()
    shutil.rmtree(bundle.base_dir, ignore_errors=True)  # fresh-worker simulation
    before = s3_request_counts()
    t0 = time.perf_counter()
    bundle.initialize()
    elapsed = time.perf_counter() - t0
    # MinIO's metrics endpoint lags a few seconds; poll until the counters move.
    deltas: dict[str, int] = {}
    for _ in range(15):
        deltas = diff(before, s3_request_counts())
        if deltas:
            break
        time.sleep(1)
    staged = sum(1 for p in bundle.path.rglob("*.py") if p.is_file())
    return elapsed, deltas, staged


def scenario(label: str, make_bundle) -> dict:
    timings, last_reqs, staged = [], {}, 0
    for i in range(ROUNDS):
        elapsed, reqs, staged = run_cold(make_bundle)
        timings.append(elapsed)
        last_reqs = reqs
        pretty = ", ".join(f"{k}={v}" for k, v in sorted(last_reqs.items())) or "metrics n/a"
        print(f"  round {i + 1}: {elapsed:6.2f}s   ({pretty})")
    return {"label": label, "median": statistics.median(timings), "reqs": last_reqs, "staged": staged}


def main() -> None:
    print(f"Cold-staging benchmark, {ROUNDS} rounds each (local dir wiped before every round)\n")

    print("[1/2] stock S3DagBundle — serial per-object sync")
    stock = scenario(
        "per-object sync (stock S3DagBundle)",
        lambda: S3DagBundle(
            name="bench-per-object", bucket_name=BUCKET, prefix=PREFIX, aws_conn_id="aws_default"
        ),
    )

    print("\n[2/2] S3ArchiveDagBundle — single tar.gz object")
    archive = scenario(
        "single archive (S3ArchiveDagBundle)",
        lambda: S3ArchiveDagBundle(
            name="bench-archive",
            bucket_name=BUCKET,
            archive_key=ARCHIVE_KEY,
            aws_conn_id="aws_default",
        ),
    )

    total = lambda r: sum(v for k, v in r["reqs"].items() if any(a in k for a in INTERESTING_APIS))  # noqa: E731
    print(f"\n{'=' * 74}")
    print(f"{'scenario':<40}{'median':>9}{'S3 reqs':>10}{'files':>8}")
    print(f"{'-' * 74}")
    for r in (stock, archive):
        reqs = total(r) if r["reqs"] else "n/a"
        print(f"{r['label']:<40}{r['median']:>8.2f}s{reqs!s:>10}{r['staged']:>8}")
    print(f"{'=' * 74}")
    if archive["median"] > 0:
        print(
            f"\nspeedup: {stock['median'] / archive['median']:.1f}x   "
            f"(every KubernetesExecutor task pod pays the cold-staging cost)"
        )


if __name__ == "__main__":
    main()
