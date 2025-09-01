#!/usr/bin/env bash
set -euo pipefail

TS=$(date +"%Y%m%d_%H%M%S")
OUT_DIR="$(pwd)/out"
mkdir -p "$OUT_DIR"

echo "[1/6] Levantando Postgres con docker-compose (solo DB)…"
docker compose up -d db

echo "[2/6] Esperando a que Postgres acepte conexiones…"
until docker compose exec -T db pg_isready -U app -d spapi >/dev/null 2>&1 ; do
  printf "."
  sleep 1
done
sleep 2
echo " ok"

DC_RUN="docker compose run --rm -v $OUT_DIR:/out"

echo "[3/6] Caso 1: mixed_cases.json"
$DC_RUN \
  -e PG_DSN="postgresql://app:app@db:5432/spapi" \
  -e MOCK_MODE=1 \
  -e MOCK_FILE=/app/sample_data/mixed_cases.json \
  app python main.py --posted-after 2025-08-01T00:00:00Z --summary-csv /out/summary_mixed_${TS}.csv

echo "[4/6] Caso 2: rich_payload.json"
$DC_RUN \
  -e PG_DSN="postgresql://app:app@db:5432/spapi" \
  -e MOCK_MODE=1 \
  -e MOCK_FILE=/app/sample_data/rich_payload.json \
  app python main.py --posted-after 2025-08-01T00:00:00Z --summary-csv /out/summary_rich_${TS}.csv

echo "[5/6] Caso 3: paginado (page1 + page2)"
$DC_RUN \
  -e PG_DSN="postgresql://app:app@db:5432/spapi" \
  -e MOCK_MODE=1 \
  -e MOCK_DIR=/app/sample_data \
  app python main.py --posted-after 2025-08-01T00:00:00Z --summary-csv /out/summary_paged_${TS}.csv

echo "[6/6] Listado de CSVs generados:"
ls -l "$OUT_DIR"/*.csv || true

echo "OK: smoke suite completada."
