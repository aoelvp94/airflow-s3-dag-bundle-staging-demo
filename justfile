# End-to-end reproduction of the per-object DAG-bundle staging cost.
# Requires: astro CLI, Docker, python3. Run `just e2e` for the whole thing.

set shell := ["bash", "-cu"]

n_files := "400"
avg_kb := "8"

# List recipes
default:
    @just --list

# Generate the synthetic bundle (N small files + one tar.gz of the same files)
generate:
    python3 scripts/generate_bundle.py {{n_files}} {{avg_kb}}

# Build the image and start Airflow + MinIO
up:
    astro dev start --no-browser

# Create the bucket and load BOTH layouts: per-object files and the archive
seed:
    #!/usr/bin/env bash
    set -euo pipefail
    minio_id=$(docker ps -qf name=minio | head -1)
    [ -n "$minio_id" ] || { echo "MinIO is not running — run 'just up' first"; exit 1; }
    network=$(docker inspect "$minio_id" --format '{{{{range $k, $v := .NetworkSettings.Networks}}{{{{$k}}{{{{end}}')
    docker run --rm --network "$network" -v "$PWD/bundle_build:/build:ro" \
        --entrypoint sh pgsty/silo:RELEASE.2026-10-26T00-00-00Z -c '
        mc alias set demo http://minio:9000 minioadmin minioadmin &&
        mc mb -p demo/dag-bundles &&
        mc mirror --overwrite --remove /build/dags demo/dag-bundles/dags &&
        mc cp /build/dag-bundle.tar.gz demo/dag-bundles/archive/dag-bundle.tar.gz &&
        echo && mc du demo/dag-bundles/dags && mc ls demo/dag-bundles/archive/'

# Time cold staging of both strategies and count real S3 requests (3 rounds)
bench rounds="3":
    #!/usr/bin/env bash
    set -euo pipefail
    scheduler=$(docker ps -qf name=scheduler | head -1)
    [ -n "$scheduler" ] || { echo "Scheduler is not running — run 'just up' first"; exit 1; }
    docker exec "$scheduler" python /usr/local/airflow/include/benchmark.py {{rounds}}

# Show how the DAG processor refreshed the two live bundles.
# Archive staging logs at debug ("Staged Dag bundle from archive").
logs:
    #!/usr/bin/env bash
    set -euo pipefail
    dp=$(docker ps -qf name=dag-processor | head -1)
    [ -n "$dp" ] || dp=$(docker ps -qf name=scheduler | head -1)
    docker logs "$dp" 2>&1 | grep -iE "staged|refreshing bundle|archive|downloading dags|etag" | tail -40

# Host check of the vendored upstream bundle module (no Docker)
check:
    python3 -m unittest tests.test_vendored_s3_bundle

# Watch every S3 API call MinIO serves, live (Ctrl+C to stop)
trace:
    #!/usr/bin/env bash
    set -euo pipefail
    minio_id=$(docker ps -qf name=minio | head -1)
    network=$(docker inspect "$minio_id" --format '{{{{range $k, $v := .NetworkSettings.Networks}}{{{{$k}}{{{{end}}')
    docker run --rm -it --network "$network" --entrypoint sh pgsty/silo:RELEASE.2026-10-26T00-00-00Z -c \
        'mc alias set demo http://minio:9000 minioadmin minioadmin && mc admin trace --call s3 demo'

# The whole reproduction: generate -> start -> seed -> benchmark
e2e: generate up seed bench

# Stop everything
down:
    astro dev stop

# Stop and delete containers, generated files, and local state
clean: down
    astro dev kill || true
    rm -rf bundle_build
