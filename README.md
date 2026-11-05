# Airflow S3 DAG bundle staging: per-object sync vs single archive

A self-contained, fully local reproduction of a performance gap in Airflow 3's
object-store DAG bundles: **`S3DagBundle` stages the bundle one object at a
time, serially** — so a DAG project made of hundreds of small files pays one
S3 round-trip (HEAD + GET) *per file*, on *every fresh worker*. Under
KubernetesExecutor every task instance is a fresh worker, which puts the full
staging cost on the critical path of every task start.

Everything runs on your machine: `astro dev` (Astronomer's local Airflow) plus
a [Silo](https://github.com/pgsty/silo) container (MinIO-compatible S3; the
`minio/minio` and `minio/mc` Docker Hub repositories are gone). **No AWS
account, no cloud requests, nothing is uploaded anywhere.**

```
per-object sync (stock)                  single archive (this PoC)

worker start                             worker start
  │                                        │
  ├─ LIST prefix                           ├─ HEAD archive.tar.gz
  ├─ HEAD file_001 ── GET file_001  ─┐     ├─ GET  archive.tar.gz  ─┐
  ├─ HEAD file_002 ── GET file_002   │     ├─ verify + unpack        │ ~fast
  ├─ ...                             │slow ├─ atomic rename swap    ─┘
  ├─ HEAD file_400 ── GET file_400  ─┘     task code runs
  task code runs
```

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

Upstream [apache/airflow#73015](https://github.com/apache/airflow/pull/73015)
splits that strategy into its own class. This demo overlays the PR tip
(`ad0592a555cdc1110e006b5c8b989a5e02e634a6`, `vendor/s3.py`) onto the Runtime
image, because Astro Runtime 3.3-7 does not ship it yet. The archive classpath
is `airflow.providers.amazon.aws.bundles.s3.S3ArchiveDagBundle` with
`archive_key` and no `prefix`. `S3DagBundle` is only the per-object prefix
sync again. The archive is the only source of those DAGs: one `.tar.gz`
(conditional on ETag), unpacked safely, swapped into place. There is no second
copy to keep in sync and no fallback between the two.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) (`brew install astro`)
- [just](https://github.com/casey/just) (`brew install just`)
- Python 3.10+ on the host (stdlib only, used to generate the test bundle)

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

Each benchmark round deletes the bundle's local directory first — that's the
fresh-worker simulation. Sample output (Apple Silicon laptop, everything on
localhost — real S3 adds ~an order of magnitude more per-request latency):

```
Cold-staging benchmark, 3 rounds each (local dir wiped before every round)

[1/2] stock S3DagBundle — serial per-object sync
  round 1:  13.89s   (getobject=400, headbucket=1, headobject=400, listobjectsv1=1, listobjectsv2=1)
  round 2:  15.84s   (getobject=400, headbucket=1, headobject=400, listobjectsv1=1, listobjectsv2=1)
  round 3:  23.32s   (getobject=400, headbucket=1, headobject=400, listobjectsv1=1, listobjectsv2=1)

[2/2] S3ArchiveDagBundle — single tar.gz object
  round 1:   3.21s   (getobject=1, headbucket=1, headobject=2)
  round 2:   0.87s   (getobject=1, headbucket=1, headobject=2)
  round 3:   1.48s   (getobject=1, headbucket=1, headobject=3, listobjectsv1=1)

==========================================================================
scenario                                   median   S3 reqs   files
--------------------------------------------------------------------------
per-object sync (stock S3DagBundle)        15.84s       802     400
single archive (S3ArchiveDagBundle)         1.48s         5     400
==========================================================================

speedup: 10.7x   (every KubernetesExecutor task pod pays the cold-staging cost)
```

(Real run, Docker Desktop on a laptop, against the earlier in-tree subclass.
Round-to-round variance is Docker noise; the request counts are exact and
perfectly stable: 400 GET + 400 HEAD + listings vs one HEAD + one GET. The
occasional stray request in a round is the live DAG processor refreshing its
own bundles against the same MinIO.)

Re-run against PR #73015 tip `ad0592a` (`S3ArchiveDagBundle` overlaid on
Runtime 3.3-7, Silo on the loopback). `just bench` (3 cold rounds, local dir
wiped each time). The table's "S3 reqs" column is GET + HEAD + LIST only, so
it omits the bucket HEAD:

| scenario | rounds (wall) | median | last-round requests | files |
|---|---|---|---|---|
| `S3DagBundle` prefix | 2.14s, 2.13s, 2.30s | 2.14s | get=400, head=400, list v1=1, list v2=1, head-bucket=1 | 400 |
| `S3ArchiveDagBundle` | 0.12s, 0.11s, 0.13s | 0.12s | get=1, head=1, head-bucket=1 | 400 |

Speedup on that run: 18.2×. Round 1 of the prefix sync also counted the DAG
processor's own refresh (get=801, head=802). A separate warm pass, same
objects, directory left in place: prefix 0.065s (list only, no GET) and
archive 0.004s (one HEAD of the archive, no GET). The live `s3-archive` bundle
parsed `demo_dag.py` after `just seed`.

Note the per-object numbers here are against a *loopback* MinIO — real S3
adds ~25–40 ms of network round-trip per request, which is why the same
400-file bundle costs 20+ seconds per pod in a real deployment. The cost
scales linearly with file count and per-request latency, not with bytes.

## Poke around

```bash
just logs    # how the DAG processor staged the two live bundles
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
- The DAG processor logs an error for the `s3-archive` bundle until `just seed`
  has run (the archive object is missing, so `HeadObject` fails). `just e2e`
  necessarily starts Airflow before seeding (the seed step uploads to MinIO,
  which `up` brings up), so the window is expected; it clears on the next
  refresh once the archive exists. The `s3-per-object` bundle is unaffected.

## Where the behaviour lives upstream

Overlaid from [apache/airflow#73015](https://github.com/apache/airflow/pull/73015)
tip [`ad0592a555cdc1110e006b5c8b989a5e02e634a6`](https://github.com/aoelvp94/airflow/blob/ad0592a555cdc1110e006b5c8b989a5e02e634a6/providers/amazon/src/airflow/providers/amazon/aws/bundles/s3.py)
(`vendor/s3.py`):

- `S3DagBundle.refresh()` → [`S3Hook.sync_to_local_dir()`](https://github.com/apache/airflow/blob/main/providers/amazon/src/airflow/providers/amazon/aws/hooks/s3.py):
  a serial `for obj in bucket.objects.filter(...)` loop, one blocking
  `download_file` per object, no concurrency knob.
- `S3ArchiveDagBundle.refresh()`: one `HEAD` (skip when the ETag marker matches
  and the local tree is non-empty), one `GET` of `archive_key`, unpack with
  `tarfile` `filter="data"`, atomic directory swap. Configured by its own
  classpath; `archive_key` is not a `S3DagBundle` argument.
- The GCS bundle ([`GCSDagBundle`](https://github.com/apache/airflow/blob/main/providers/google/src/airflow/providers/google/cloud/bundles/gcs.py))
  ports the same per-object loop; an Azure Blob bundle with the same shape is in review
  ([apache/airflow#67016](https://github.com/apache/airflow/pull/67016)).

## Layout

```
vendor/s3.py           PR #73015 tip, overlaid onto the amazon provider in the image
scripts/generate_bundle.py   synthetic bundle generator (host, stdlib only)
include/benchmark.py   in-container benchmark (timings + MinIO request counts)
docker-compose.override.yml  adds MinIO to astro's compose stack
.env                   AWS connection (→ MinIO) + live bundle config; local-only values
justfile               all of the above, wired end to end
```

## Notes

- Members are extracted relative to the bundle root, so pack the *contents* of
  the DAGs directory (`tar -C dags .`) rather than the directory itself. The
  benchmark relies on both strategies producing an identical tree so the
  comparison is like-for-like.
- The PoC extracts with `tarfile`'s `filter="data"` (path-traversal safe) and
  swaps directories by rename, keeping partially-staged states invisible.
- `prefix` belongs to `S3DagBundle` and `archive_key` belongs to
  `S3ArchiveDagBundle`. Passing `archive_key` to the prefix class, or `prefix`
  to the archive class, raises `TypeError` at construction. Delete the archive
  object and rerun `just bench` to watch the archive bundle fail loudly
  instead of silently staging from somewhere else — there is no per-object
  fallback.
