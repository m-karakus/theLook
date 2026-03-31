#!/bin/bash
set -euo pipefail

echo "=== ELT Stack Webserver Init ==="
echo "Environment: ${ENV:-prod}"

# -- Step 1: Inject credentials --
echo "[1/4] Injecting credentials..."
python .credentials_update.py --env "${ENV:-prod}"

# -- Step 2: Setup DAGSTER_HOME --
echo "[2/4] Setting up DAGSTER_HOME..."
mkdir -p "${DAGSTER_HOME}/storage" "${DAGSTER_HOME}/local" "${DAGSTER_HOME}/logs"
cp dagster.yaml "${DAGSTER_HOME}/dagster.yaml"

# -- Step 3: dbt deps + parse manifest --
echo "[3/4] Running dbt deps + parse..."
cd dbt_project
dbt deps --quiet --profiles-dir .dbt
dbt parse --quiet --profiles-dir .dbt
cd ..
echo "  Manifest generated: dbt_project/target/manifest.json"

# -- Step 4: Start dagster-webserver --
echo "[4/4] Starting dagster-webserver..."
exec dagster-webserver -w workspace.yaml -h 0.0.0.0 -p 3000
