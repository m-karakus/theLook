#!/bin/bash
set -euo pipefail

echo "=== ELT Stack Daemon Init ==="
echo "Environment: ${ENV:-prod}"

# -- Step 1: Inject credentials --
echo "[1/3] Injecting credentials..."
python .credentials_update.py --env "${ENV:-prod}"

# -- Step 2: Setup DAGSTER_HOME --
echo "[2/3] Setting up DAGSTER_HOME..."
mkdir -p "${DAGSTER_HOME}/storage" "${DAGSTER_HOME}/local" "${DAGSTER_HOME}/logs"
cp dagster.yaml "${DAGSTER_HOME}/dagster.yaml"

# Wait for webserver to generate dbt manifest (shared volume)
echo "  Waiting for dbt manifest..."
timeout=120
elapsed=0
while [ ! -f dbt_project/target/manifest.json ]; do
    sleep 2
    elapsed=$((elapsed + 2))
    if [ "$elapsed" -ge "$timeout" ]; then
        echo "ERROR: manifest.json not found after ${timeout}s"
        exit 1
    fi
done
echo "  Manifest found."

# -- Step 3: Start dagster-daemon --
echo "[3/3] Starting dagster-daemon..."
exec dagster-daemon run
