.PHONY: install run test build compose-up compose-down compose-run smoke-suite \
        db-up db-down test-int test-e2e test-all run-real run-mock run-mock-paged env-check

# -----------------------
# Variables (with defaults)
# -----------------------
PG_DSN ?= postgresql://app:app@localhost:55432/spapi
PYTHONPATH := $(shell pwd)

# -----------------------
# Dev helpers (local)
# -----------------------
install:
	pip install -r requirements.txt

run:
	python main.py --posted-after $${POSTED_AFTER:-2025-08-01T00:00:00Z} $${MARKETPLACE_ID:+--marketplace-id $${MARKETPLACE_ID}} $${POSTED_BEFORE:+--posted-before $${POSTED_BEFORE}} $${SUMMARY_CSV:+--summary-csv $${SUMMARY_CSV}}

test:
	PYTHONPATH=$(PYTHONPATH) pytest -q

# -----------------------
# Docker Compose
# -----------------------
build:
	docker compose build app --no-cache

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down -v

compose-run:
	docker compose run --rm app python main.py --posted-after $${AFTER:-2025-08-01T00:00:00Z} $${MPID:+--marketplace-id $${MPID}} $${BEFORE:+--posted-before $${BEFORE}} $${CSV:+--summary-csv $${CSV}}

# -----------------------
# Smoke suite (3 mock runs + CSVs)
# -----------------------
smoke-suite:
	./scripts/smoke_suite.sh

# -----------------------
# Integration & E2E tests
# -----------------------

db-up:
	docker compose up -d db

db-down:
	docker compose down -v

# Integration against local DB (host -> compose DB)
test-int: db-up
	PYTHONPATH=$(PYTHONPATH) PG_DSN=$(PG_DSN) pytest -q -m integration tests/test_integration_db.py

# E2E run (CLI inside Docker)
test-e2e: db-up
	PYTHONPATH=$(PYTHONPATH) pytest -q -m integration tests/test_cli_e2e.py

# Both integration + E2E
test-all: db-up
	PYTHONPATH=$(PYTHONPATH) PG_DSN=$(PG_DSN) pytest -q -m integration tests/test_integration_db.py
	PYTHONPATH=$(PYTHONPATH) pytest -q -m integration tests/test_cli_e2e.py

# -----------------------
# Real run (SP-API live)
# -----------------------
run-real:
	docker compose run --rm \
		-v "$(PWD)/out":/out \
		app python main.py \
		--posted-after $${POSTED_AFTER:-2025-08-01T00:00:00Z} \
		--summary-csv /out/summary.csv

# Mock mode: use sample_data/rich_payload.json and generate summary.csv + validation_report.csv in ./out
run-mock:
	docker compose run --rm \
		-v "$(PWD)/out":/out \
		-e MOCK_MODE=1 \
		-e MOCK_FILE=/app/sample_data/rich_payload.json \
		app python main.py \
		--posted-after $${POSTED_AFTER:-2025-08-01T00:00:00Z} \
		--summary-csv /out/summary.csv

# Mock mode with pagination (reads sample_data/page1.json + page2.json)
run-mock-paged:
	docker compose run --rm \
		-v "$(PWD)/out":/out \
		-e MOCK_MODE=1 \
		-e MOCK_DIR=/app/sample_data \
		app python main.py \
		--posted-after $${POSTED_AFTER:-2025-08-01T00:00:00Z} \
		--summary-csv /out/summary.csv

# Show environment variables inside the 'app' container
env-check:
	docker compose run --rm app /bin/sh -lc 'env | grep -E "^(LWA|AWS|SPAPI|PG_DSN|VALIDATION_CSV)=" || true'
