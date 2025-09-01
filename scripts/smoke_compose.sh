#!/usr/bin/env bash
set -euo pipefail
echo "[1/3] Levantando stack (db + app build)…"
docker compose up -d --build db
echo "[2/3] Ejecutando app con MOCK y export CSV…"
docker compose run --rm \
  -e MOCK_MODE=1 \
  -e MOCK_DIR=/app/sample_data \
  -e MOCK_SCENARIO=429_twice_then_200 \
  app python main.py --posted-after 2025-08-01T00:00:00Z --summary-csv /app/smoke_summary.csv
echo "OK: Smoke test en compose completado."
