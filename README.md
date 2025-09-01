# SP-API Transactions Ingestor

Technical exercise – Numetri (Python + Postgres + Amazon SP‑API)

This service fetches transactions from **Amazon Selling Partner API** (Finances → `listTransactions`),
handles API errors, persists data into **Postgres**, produces a **per‑SKU summary**, and outputs a **validation report** (missing/duplicates).

---

## ✅ Delivery checklist (what’s covered)

- **SP‑API connection (mock & real)**  
  - Real: AWS SigV4 signing + LWA access token; pagination via `nextToken`.
  - Mock: local JSON payloads in `sample_data/` with 403/429 simulators and paginated pages.
- **Error handling (401, 403, 429, 500/503)**  
  - 403 → refresh LWA token and retry once  
  - 429 → exponential backoff with mild jitter  
  - 500/503 → short retry  
  - Empty 200 → defensive parsing
- **Postgres persistence**  
  - Tables: `sp_transactions` (header) and `sp_transaction_items` (items)  
  - Idempotency: `UNIQUE` + `ON CONFLICT DO NOTHING`  
  - Indexes on `sku`; connection auto‑reopen on `psycopg.OperationalError`
- **SKU summary**  
  - `summarize_by_sku()` sums item totals per SKU; avoids double counting when items exist  
  - Export to CSV with `--summary-csv`
- **Validation of missing/duplicates (logs + CSV)**  
  - `run_validations()`: `transactions_missing_sku`, `items_missing_sku`, `duplicate_transactions`, `duplicate_items`, `orphan_items`  
  - Logged and optionally exported to CSV if `VALIDATION_CSV` is set

---

## 📦 Requirements

- Docker + Docker Compose
- Make
- (Optional) Python 3.11+ if you want to run locally

---

## ⚙️ Setup

1) Copy `.env.example` to `.env` and fill real credentials when available.  
2) Ensure the output folder exists:
```bash
mkdir -p out
```

**Important env keys (examples):**
```ini
# LWA
LWA_CLIENT_ID=...
LWA_CLIENT_SECRET=...
LWA_REFRESH_TOKEN=...

# AWS SigV4
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=

SPAPI_REGION=us-east-1
SPAPI_HOST=sellingpartnerapi-na.amazon.com
MARKETPLACE_ID=ATVPDKIKX0DER

# Postgres DSN (compose)
PG_DSN=postgresql://app:app@db:5432/spapi
# Local host (no Docker): postgresql://app:app@localhost:55432/spapi

# Validation CSV path (recommended under /out inside containers)
VALIDATION_CSV=/out/validation_report.csv
```

---

## ▶️ How to run

### 1) Mock mode (no credentials)
Deterministic data from `sample_data/`, generates CSVs quickly.

```bash
make run-mock          # single JSON payload
make run-mock-paged    # reads page1.json + page2.json
```

Outputs to `./out`:
- `summary.csv`
- `validation_report.csv`

---

### 2) Real mode WITH Docker (recommended)
Requires `.env` with LWA/AWS credentials and `PG_DSN=postgresql://app:app@db:5432/spapi`.

```bash
# Start DB only (if not up yet)
docker compose up -d db

# Run the CLI
docker compose run --rm   -v "$(pwd)/out":/out   app python main.py   --posted-after "2025-08-01T00:00:00Z"   --posted-before "2025-08-31T23:59:59Z"   --marketplace-id "$MARKETPLACE_ID"   --summary-csv /out/summary.csv
```

Makefile shortcut (equivalent):
```bash
make run-real POSTED_AFTER=2025-08-01T00:00:00Z
```

Outputs to `./out`:
- `summary.csv`
- `validation_report.csv`

---

### 3) Real mode WITHOUT Docker (local host)

```bash
make install
export $(grep -v '^#' .env | xargs)

python3 main.py   --posted-after "2025-08-01T00:00:00Z"   --posted-before "2025-08-31T23:59:59Z"   --marketplace-id "$MARKETPLACE_ID"   --summary-csv summary.csv
```

Outputs to current folder:
- `summary.csv`
- `validation_report.csv` (if `VALIDATION_CSV=validation_report.csv`)

> If you use the compose DB from host, expose `55432:5432` in the compose file and set `PG_DSN=postgresql://app:app@localhost:55432/spapi`.

---

## 🔬 Tests & smoke

```bash
make test        # unit + integration mix
make test-int    # integration (DB in Docker)
make test-e2e    # E2E (CLI in container)
make test-all    # both
make smoke-suite # 3 mock scenarios → CSVs under ./out
```

Smoke artifacts:
- `out/summary_mixed_<timestamp>.csv`
- `out/summary_rich_<timestamp>.csv`
- `out/summary_paged_<timestamp>.csv`

---

## 🧰 Useful debug commands

```bash
# Show relevant envs inside the app container
docker compose run --rm app env | grep -E "^(LWA|AWS|SPAPI|MARKETPLACE_ID|PG_DSN|VALIDATION_CSV)="

# Inspect DB tables
docker compose exec db psql -U app -d spapi -c "\dt+"
```

---
