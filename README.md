# Airflow S3 DAG bundle staging: per-object sync vs single archive

A self-contained, fully local reproduction of a performance gap in Airflow 3's
object-store DAG bundles: **`S3DagBundle` stages the bundle one object at a
time, serially** — so a DAG project made of hundreds of small files pays one
S3 round-trip (HEAD + GET) *per file*, on *every fresh worker*. Under
KubernetesExecutor every task instance is a fresh worker, which puts the full
staging cost on the critical path of every task start.

Everything runs on your machine: `astro dev` (Astronomer's local Airflow) plus
a [Silo](https://github.com/pgsty/silo) container standing in for S3
(MinIO-compatible; `minio/minio` and `minio/mc` are gone from Docker Hub).
**No AWS account, no cloud requests, nothing is uploaded anywhere.**

```
per-object sync (S3DagBundle)            single archive (S3ArchiveDagBundle)

worker start                             worker start
  │                                        │
  ├─ LIST prefix                           ├─ HEAD archive.tar.gz
  ├─ HEAD file_001 ── GET file_001  ─┐     ├─ GET  archive.tar.gz  ─┐
  ├─ HEAD file_002 ── GET file_002   │     ├─ verify + unpack        │ ~fast
  ├─ ...                             │slow ├─ atomic rename swap    ─┘
  ├─ HEAD file_400 ── GET file_400  ─┘     task code runs
  task code runs
```

## Classpath

The image installs `apache-airflow-providers-amazon==9.37.0` (the newest
release whose `common-compat` floor matches Astro Runtime 3.3-7) and overlays
[`vendor/s3.py`](vendor/s3.py), a verbatim copy of
[`airflow.providers.amazon.aws.bundles.s3`](https://github.com/apache/airflow/blob/602dc7b533f2b4732dc4bcb78a6f716731fc4d16/providers/amazon/src/airflow/providers/amazon/aws/bundles/s3.py)
at commit `602dc7b` ([apache/airflow#73015](https://github.com/apache/airflow/pull/73015),
`aoelvp94/airflow` branch `s3-dag-bundle-archive-staging`).
Provenance and the sha256 are in [`vendor/UPSTREAM.txt`](vendor/UPSTREAM.txt).

Both the live DAG processor config and `include/benchmark.py` import that module:

| bundle | classpath | kwargs |
|---|---|---|
| per-object | `airflow.providers.amazon.aws.bundles.s3.S3DagBundle` | `bucket_name`, `prefix`, `aws_conn_id` |
| archive | `airflow.providers.amazon.aws.bundles.s3.S3ArchiveDagBundle` | `bucket_name`, `archive_key`, `aws_conn_id` |

`S3DagBundle` is the prefix sync. `S3ArchiveDagBundle` is a separate class,
configured by its own classpath, and it takes `archive_key` (the `.tar.gz`
object). They share connection and local-directory setup through a private
base. The archive is the only source of DAGs for the archive bundle.

Behavior from that commit, which the host test (`just check`) locks in:

- The ETag marker lives **inside** the staged tree, at `{s3_dags_dir}/.archive_etag`
  (for example `.../s3-archive/.archive_etag`). `_bundle_dir_has_files()` ignores
  that marker, so a directory that contains only `.archive_etag` is treated as
  empty and restaged.
- A refresh skips the download when the marker matches the object ETag and the
  tree still has other files. It restages when the marker is missing or the
  tree has no files besides the marker (the cold-worker case, including a
  wiped local directory).
- The download is one `GetObject` with `If-Match`, streamed with
  `shutil.copyfileobj` in 1 MiB chunks, then unpacked with `tarfile`
  `filter="data"` and swapped into place by rename. The constructor checks
  `tarfile.data_filter` once; extract does not check again.
- `refresh()` on the shared base rejects a versioned refresh, takes the bundle
  lock, and calls `_refresh_locked()`. Loggers come from `_get_logger()`.
- Staging leftovers live beside the tree, under `.{name}/archive-staging-*`,
  and are removed on the next refresh.
- `S3DagBundle.refresh()` does not unlink `.archive_etag` itself. It syncs
  with `delete_stale=True`, so a prefix refresh removes the marker when the
  remote prefix does not contain that name. Pointing the same bundle name
  back at an archive then restages.

## Why this matters

DAG projects are many tiny files, not few big ones. Moving a few MB is not a
bandwidth problem — it's a *request count* problem: each object costs a full
round-trip (~20–50 ms against real S3 from in-region compute), issued serially,
and each `download_file` also HEADs before it GETs. Multipart parallelism
(`TransferConfig`) never kicks in because the files are far below the
threshold.

Local caching (ETag/mtime skips) only helps long-lived components like the
scheduler. Ephemeral workers start with an empty disk every time, so:

> staging cost × tasks/day = a tax on every single task in the deployment

For a hypothetical-but-typical setup — 400 files, ~8 KiB average,
KubernetesExecutor, 10k task instances/day — that's millions of billable S3
requests per day and tens of pod-hours of pure waiting, to repeatedly move
~3 MiB. Every other system that ships code to ephemeral workers ships **one
compressed artifact** (Spark archives, Lambda zips, container image layers);
Airflow's `GitDagBundle` gets this implicitly, the object-store bundles don't
— and the same serial loop is being ported to GCS and Azure bundles.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) (`brew install astro`)
- [just](https://github.com/casey/just) (`brew install just`)
- Python 3.10+ on the host (stdlib only, used to generate the test bundle and to run `just check`)

## Reproduce (one command)

```bash
just e2e
```

That runs, in order:

| step | recipe | what it does |
|---|---|---|
| 1 | `just generate` | builds `bundle_build/`: 400 small `.py` files (~3 MiB) **and** a `.tar.gz` of the same files |
| 2 | `just up` | `astro dev start`: Airflow 3 + a MinIO container (fake S3) |
| 3 | `just seed` | creates bucket `dag-bundles`, uploads the files per-object **and** the archive — to local MinIO only |
| 4 | `just bench` | inside the container: cold-stages both bundle classes 3×, timing each and reading **actual request counts from MinIO's metrics endpoint** |

Each benchmark round deletes the bundle's local directory first. `.archive_etag`
lives inside that directory, so the wipe removes the marker. That is the
fresh-worker simulation: the archive class sees neither a marker nor Dag files
and restages. Measured with `just bench` on loopback Silo (Astro Runtime 3.3-7,
Airflow 3.3.1+astro.4, amazon provider 9.37.0, `vendor/s3.py` from `602dc7b`).
Real S3 adds ~an order of magnitude more per-request latency:

```
Cold-staging benchmark, 3 rounds each (local dir wiped before every round)

[1/2] stock S3DagBundle — serial per-object sync
  round 1:   2.15s   (getobject=400, headbucket=1, headobject=400, listobjectsv1=1, listobjectsv2=1)
  round 2:   2.17s   (getobject=400, headbucket=1, headobject=400, listobjectsv1=1, listobjectsv2=1)
  round 3:   2.04s   (getobject=400, headbucket=1, headobject=400, listobjectsv1=1, listobjectsv2=1)

[2/2] S3ArchiveDagBundle — single tar.gz object
  round 1:   0.14s   (getobject=1, headbucket=1, headobject=1)
  round 2:   0.12s   (getobject=1, headbucket=1, headobject=1)
  round 3:   0.14s   (getobject=1, headbucket=1, headobject=1)

==========================================================================
scenario                                   median   S3 reqs   files
--------------------------------------------------------------------------
per-object sync (stock S3DagBundle)         2.15s       802     400
single archive (S3ArchiveDagBundle)         0.14s         2     400
==========================================================================

speedup: 15.6x   (every KubernetesExecutor task pod pays the cold-staging cost)
```

The request mix did not move between rounds. Stock cold staging is 400 GET
+ 400 HEAD, plus one ListObjectsV1 and one ListObjectsV2 (802 calls in the
table; initialization also sends a bucket HEAD). The archive classpath does
one HeadObject (ETag) and one GetObject (1 MiB `copyfileobj`) per cold stage,
plus that bucket HEAD — 2 calls in the table. These rounds had no stray
requests from the live DAG processor. Another run can pick one up when that
processor refreshes its own bundles against the same MinIO.

Loopback Silo makes each stock call about 3 ms, so 15.6× wall clock is the
floor on this machine. Real S3 adds ~25–40 ms of network round-trip per
request, which is why the same 400-file bundle costs 20+ seconds per pod in
a real deployment. The cost scales linearly with file count and per-request
latency, not with bytes.

Same run, summarized (Runtime 3.3-7, amazon provider 9.37.0, `vendor/s3.py`
from `602dc7b`, Silo, 400 files, ~3.1 MiB):

| scenario | rounds | median | requests each round | files |
|---|---|---|---|---|
| `S3DagBundle` prefix, cold | 2.15s, 2.17s, 2.04s | 2.15s | get=400, head=400, list v1=1, list v2=1, head-bucket=1 | 400 |
| `S3ArchiveDagBundle`, cold | 0.14s, 0.12s, 0.14s | 0.14s | get=1, head=1, head-bucket=1 | 400 |

The recipe's summary column counts GET + HEAD + LIST (802 vs 2) and reports a
15.6× speedup.

## Poke around

```bash
just check   # host unit test of the vendored module (marker, restage, copy size)
just logs    # how the DAG processor refreshed the two live bundles
just trace   # watch every S3 call MinIO serves, live (run during a bench)
just bench 5 # more rounds
```

The Airflow UI is at http://localhost:8080 — both bundles (`s3-per-object`
and `s3-archive`) are configured live via `AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST`
in [.env](.env) and stage the same file set from the same bucket; the
`bundle_demo` DAG registers under one of them (DAG ids are global). MinIO's
console is at http://localhost:9001 (minioadmin/minioadmin).

When you're done: `just down` (stop) or `just clean` (delete everything).

Two benign things you may see on a first run:

- `astro dev start` can report *"The api-server health check timed out"* —
  the containers keep starting anyway (`docker ps` to confirm); it's just a
  slow first boot under Docker Desktop.
- The DAG processor logs a HeadObject failure for the `s3-archive` bundle
  (the archive key is not in the bucket yet) until `just seed` has run.
  `just e2e` starts Airflow before seeding, because seed uploads to the MinIO
  that `up` brings up. The error clears on the next refresh once the archive
  object exists. The `s3-per-object` bundle is unaffected.

Archive refresh messages (`Staged Dag bundle from archive ...`) are logged at
debug. `just logs` filters for them; raise the component log level to debug
if the processor is quiet at info.

## Where the behaviour lives upstream

- This demo runs the S3 bundle module from
  [602dc7b](https://github.com/apache/airflow/blob/602dc7b533f2b4732dc4bcb78a6f716731fc4d16/providers/amazon/src/airflow/providers/amazon/aws/bundles/s3.py)
  ([#73015](https://github.com/apache/airflow/pull/73015)): `S3DagBundle._refresh_locked()`
  calls [`S3Hook.sync_to_local_dir()`](https://github.com/apache/airflow/blob/main/providers/amazon/src/airflow/providers/amazon/aws/hooks/s3.py),
  a serial `for obj in bucket.objects.filter(...)` loop, one blocking
  `download_file` per object. `S3ArchiveDagBundle._refresh_locked()` downloads
  `archive_key` once and records the ETag at `{s3_dags_dir}/.archive_etag`.
- The GCS bundle ([`GCSDagBundle`](https://github.com/apache/airflow/blob/main/providers/google/src/airflow/providers/google/cloud/bundles/gcs.py))
  ports the same per-object loop; an Azure Blob bundle with the same shape is in review
  ([apache/airflow#67016](https://github.com/apache/airflow/pull/67016)).

## Layout

```
vendor/s3.py           verbatim S3DagBundle + S3ArchiveDagBundle from 602dc7b
vendor/UPSTREAM.txt    commit URL and sha256
include/benchmark.py   in-container benchmark (timings + MinIO request counts)
scripts/generate_bundle.py   synthetic bundle generator (host, stdlib only)
tests/test_vendored_s3_bundle.py   host test of the vendored module
docker-compose.override.yml  adds MinIO to astro's compose stack
.env                   AWS connection (→ MinIO) + live bundle config; local-only values
Dockerfile             installs the Amazon provider and overlays vendor/s3.py
justfile               all of the above, wired end to end
```

## Notes

- Members are extracted relative to the bundle root, so pack the *contents* of
  the DAGs directory (`tar -C dags .`) rather than the directory itself. The
  benchmark relies on both strategies producing an identical tree so the
  comparison is like-for-like.
- The archive class extracts with `tarfile`'s `filter="data"` (path-traversal
  safe; requires Python 3.11.4+, which Astro Runtime 3.3 has) and swaps
  directories by rename, keeping partially-staged states invisible. Staging
  leftovers live under `.{name}/archive-staging-*` and are removed on the
  next refresh. The ETag marker is `.archive_etag` inside the staged tree.
- Give the per-object bundle a `prefix` and the archive bundle an
  `archive_key`. Those are parameters of two classes. Removing the archive
  object and rerunning `just bench` makes `S3ArchiveDagBundle` fail on
  HeadObject; it has no second source to stage from.
