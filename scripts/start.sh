#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export DAGSTER_HOME="$PROJECT_ROOT/.dagster"

mkdir -p "$DAGSTER_HOME"

echo "DAGSTER_HOME=$DAGSTER_HOME"
echo "Iniciando Dagster em http://localhost:3000 ..."
cd "$PROJECT_ROOT"
uv run dagster dev
