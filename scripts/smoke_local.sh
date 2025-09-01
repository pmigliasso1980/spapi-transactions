#!/usr/bin/env bash
set -euo pipefail
echo "[1/3] Levantando Postgres con docker-compose…"
docker compose up -d db
echo "[2/3] Ejecutando ingesta MOCK (paginado sample_data) y exportando CSV…"
export PG_DSN="postgresql://app:app@localhost:5432/spapi"
export MOCK_MODE=1
export MOCK_DIR="./sample_data"
export MOCK_SCENARIO="429_twice_then_200"
python main.py --posted-after 2025-08-01T00:00:00Z --summary-csv smoke_summary.csv
echo "[3/3] Primeras líneas del CSV:"
head -n 10 smoke_summary.csv || true
echo "OK: Smoke test local completado."
