import os, json, time, argparse, logging, csv
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv
import requests
from requests_aws4auth import AWS4Auth
import psycopg

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
LOG = logging.getLogger("spapi")

SPAPI_VERSION = "2024-06-19"
TOKEN_URL = "https://api.amazon.com/auth/o2/token"


# -------------------- Utils --------------------

def env(n: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(n, default)
    if required and not v:
        raise RuntimeError(f"Missing env: {n}")
    return v or ""


def get_awsauth() -> AWS4Auth:
    return AWS4Auth(
        env("AWS_ACCESS_KEY_ID", required=True),
        env("AWS_SECRET_ACCESS_KEY", required=True),
        env("SPAPI_REGION", "us-east-1"),
        "execute-api",
        session_token=env("AWS_SESSION_TOKEN") or None,
    )


def get_lwa_access_token() -> str:
    data = {
        "grant_type": "refresh_token",
        "refresh_token": env("LWA_REFRESH_TOKEN", required=True),
        "client_id": env("LWA_CLIENT_ID", required=True),
        "client_secret": env("LWA_CLIENT_SECRET", required=True),
    }
    r = requests.post(TOKEN_URL, data=data, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


# -------------------- MOCK MODE --------------------

def list_transactions_mock(
    posted_after: str,
    posted_before: Optional[str],
    marketplace_id: Optional[str],
    transaction_status: Optional[str],
    host: str,
    awsauth: Optional[AWS4Auth] = None,
) -> Iterable[Dict[str, Any]]:
    """Emulates the API by reading local JSON files and simulating throttling/403 if requested."""
    mf = os.getenv("MOCK_FILE")
    md = os.getenv("MOCK_DIR")
    sc = os.getenv("MOCK_SCENARIO", "200_only")
    calls = {"n": 0}

    def iterdir(d):
        i = 1
        while True:
            p = os.path.join(d, f"page{i}.json")
            if not os.path.exists(p):
                break
            with open(p, "r") as fh:
                yield json.load(fh)
            i += 1

    if md and os.path.isdir(md):
        while True:
            calls["n"] += 1
            if sc == "429_twice_then_200" and calls["n"] <= 2:
                LOG.warning("MOCK: simulated 429 throttling (attempt %s)", calls["n"])
                continue
            if sc == "403_then_200" and calls["n"] == 1:
                LOG.warning("MOCK: simulated 403 → 'refresh' and retry")
                continue
            for page in iterdir(md):
                for tx in page.get("transactions", []):
                    yield tx
            break
        return

    if mf and os.path.exists(mf):
        data = json.load(open(mf))
        for tx in data.get("transactions", []):
            yield tx
        return

    # Embedded fallback
    LOG.info("MOCK: using embedded payload (2 transactions)")
    sample = {
        "transactions": [
            {
                "transactionId": "T-MOCK-001",
                "postedDate": "2025-08-01T10:00:00Z",
                "transactionType": "Charge",
                "transactionStatus": "RELEASED",
                "marketplaceDetails": {"marketplaceId": "ATVPDKIKX0DER", "marketplaceName": "Amazon.com"},
                "totalAmount": {"currencyCode": "USD", "currencyAmount": 19.99},
                "contexts": [{"sku": "SKU-DEMO-1", "asin": "ASINDEMO1"}],
                "items": [
                    {
                        "description": "Demo Product 1",
                        "totalAmount": {"currencyCode": "USD", "currencyAmount": 19.99},
                        "contexts": [{"sku": "SKU-DEMO-1", "asin": "ASINDEMO1"}],
                    }
                ],
            },
            {
                "transactionId": "T-MOCK-002",
                "postedDate": "2025-08-02T11:30:00Z",
                "transactionType": "Refund",
                "transactionStatus": "RELEASED",
                "marketplaceDetails": {"marketplaceId": "ATVPDKIKX0DER", "marketplaceName": "Amazon.com"},
                "totalAmount": {"currencyCode": "USD", "currencyAmount": -5.00},
                "productContext": {"sku": "SKU-DEMO-2", "asin": "ASINDEMO2"},
                "items": [],
            },
        ]
    }
    for tx in sample["transactions"]:
        yield tx


# -------------------- Real SP-API --------------------

def list_transactions(
    posted_after: str,
    posted_before: Optional[str],
    marketplace_id: Optional[str],
    transaction_status: Optional[str],
    host: str,
    awsauth: AWS4Auth,
) -> Iterable[Dict[str, Any]]:
    base = f"https://{host}/finances/{SPAPI_VERSION}/transactions"
    token = get_lwa_access_token()
    next_token = None
    back = 0

    while True:
        params = {"postedAfter": posted_after}
        if posted_before:
            params["postedBefore"] = posted_before
        if marketplace_id:
            params["marketplaceId"] = marketplace_id
        if transaction_status:
            params["transactionStatus"] = transaction_status
        if next_token:
            params["nextToken"] = next_token

        r = requests.get(
            base,
            params=params,
            headers={"x-amz-access-token": token, "accept": "application/json"},
            auth=awsauth,
            timeout=60,
        )

        if r.status_code == 200:
            data = r.json() if r.content else {}
            for tx in data.get("transactions", []) or []:
                yield tx
            next_token = data.get("nextToken")
            if not next_token:
                break
            continue

        if r.status_code == 403:
            LOG.warning("403: refreshing LWA and retrying once…")
            token = get_lwa_access_token()
            r2 = requests.get(
                base,
                params=params,
                headers={"x-amz-access-token": token, "accept": "application/json"},
                auth=awsauth,
                timeout=60,
            )
            if r2.status_code == 200:
                data = r2.json() if r2.content else {}
                for tx in data.get("transactions", []) or []:
                    yield tx
                next_token = r2.json().get("nextToken") if r2.content else None
                if not next_token:
                    break
                continue
            raise RuntimeError(f"Persistent 403: {r2.text}")

        if r.status_code == 429:
            back = min(60, (2 ** min(back + 1, 6)) + 0.1 * (back + 1))
            LOG.warning("429: backoff %.1fs…", back)
            time.sleep(back)
            continue

        if r.status_code in (500, 503):
            LOG.warning("%s: short retry…", r.status_code)
            time.sleep(3)
            continue

        raise RuntimeError(f"SP-API error {r.status_code}: {r.text}")


# -------------------- Postgres --------------------

DDL = """
CREATE TABLE IF NOT EXISTS sp_transactions (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT UNIQUE,
    transaction_type TEXT,
    transaction_status TEXT,
    posted_date TIMESTAMPTZ,
    marketplace_id TEXT,
    marketplace_name TEXT,
    currency_code TEXT,
    currency_amount NUMERIC,
    sku TEXT,
    asin TEXT,
    raw JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sp_transaction_items (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT REFERENCES sp_transactions(transaction_id) ON DELETE CASCADE,
    item_index INT,
    sku TEXT,
    asin TEXT,
    item_description TEXT,
    currency_code TEXT,
    currency_amount NUMERIC,
    raw JSONB NOT NULL,
    UNIQUE (transaction_id, item_index)
);

CREATE INDEX IF NOT EXISTS sp_tx_sku_idx ON sp_transactions (sku);
CREATE INDEX IF NOT EXISTS sp_tx_items_sku_idx ON sp_transaction_items (sku);
"""


def db_connect():
    return psycopg.connect(
        env("PG_DSN", required=True),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def extract_sku_asin_from_tx(tx: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    for ctx in tx.get("contexts") or []:
        s = ctx.get("sku")
        a = ctx.get("asin")
        if s or a:
            return s, a
    prod = tx.get("productContext") or {}
    if prod.get("sku") or prod.get("asin"):
        return prod.get("sku"), prod.get("asin")
    return None, None


def extract_sku_asin_from_item(item: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    for ctx in item.get("contexts") or []:
        s = ctx.get("sku")
        a = ctx.get("asin")
        if s or a:
            return s, a
    return None, None


def upsert_transaction(conn, tx: Dict[str, Any]):
    tx_id = tx.get("transactionId")
    posted = tx.get("postedDate")
    total = (tx.get("totalAmount") or {})
    if not tx_id or not posted:
        LOG.warning("Skip transaction without ID/postedDate")
        return

    cur_code = total.get("currencyCode")
    cur_amount = total.get("currencyAmount")
    sku, asin = extract_sku_asin_from_tx(tx)

    # Header
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO sp_transactions
               (transaction_id, transaction_type, transaction_status, posted_date,
                marketplace_id, marketplace_name, currency_code, currency_amount,
                sku, asin, raw)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (transaction_id) DO NOTHING""",
            (
                tx_id,
                tx.get("transactionType"),
                tx.get("transactionStatus"),
                posted,
                (tx.get("marketplaceDetails") or {}).get("marketplaceId"),
                (tx.get("marketplaceDetails") or {}).get("marketplaceName"),
                cur_code,
                cur_amount,
                sku,
                asin,
                json.dumps(tx),
            ),
        )
    conn.commit()

    # Items
    items = tx.get("items") or []
    if items:
        with conn.cursor() as cur:
            for idx, item in enumerate(items):
                isku, iasin = extract_sku_asin_from_item(item)
                itotal = item.get("totalAmount") or {}
                cur.execute(
                    """INSERT INTO sp_transaction_items
                       (transaction_id, item_index, sku, asin, item_description,
                        currency_code, currency_amount, raw)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (transaction_id, item_index) DO NOTHING""",
                    (
                        tx_id,
                        idx,
                        isku,
                        iasin,
                        item.get("description"),
                        itotal.get("currencyCode"),
                        itotal.get("currencyAmount"),
                        json.dumps(item),
                    ),
                )
        conn.commit()


def summarize_by_sku(conn) -> List[Tuple[str, float]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sku, SUM(currency_amount)::float AS total_amount
            FROM (
                SELECT sku, currency_amount
                FROM sp_transaction_items
                WHERE sku IS NOT NULL
                UNION ALL
                SELECT sku, currency_amount
                FROM sp_transactions
                WHERE sku IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM sp_transaction_items ti
                    WHERE ti.transaction_id = sp_transactions.transaction_id
                  )
            ) t
            GROUP BY sku
            ORDER BY total_amount DESC NULLS LAST
            """
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def write_summary_csv(path: str, rows: List[Tuple[str, float]]):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["sku", "total_amount"])
        for sku, total in rows:
            w.writerow([sku, total if total is not None else ""])


# -------------------- Validations --------------------

def run_validations(conn) -> dict:
    """
    Validates missing fields (null SKUs), duplicates (should not exist because of UNIQUE),
    and orphan items (should not exist because of FK).
    Returns a dict with metrics and also logs them.
    """
    results = {}
    with conn.cursor() as cur:
        # Missing SKUs
        cur.execute("SELECT COUNT(*) FROM sp_transactions WHERE sku IS NULL;")
        results["transactions_missing_sku"] = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM sp_transaction_items WHERE sku IS NULL;")
        results["items_missing_sku"] = cur.fetchone()[0]

        # Duplicates (should be 0 because of UNIQUE + ON CONFLICT)
        cur.execute("""
            SELECT COUNT(*) FROM (
              SELECT transaction_id
              FROM sp_transactions
              GROUP BY transaction_id
              HAVING COUNT(*) > 1
            ) s;
        """)
        results["duplicate_transactions"] = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM (
              SELECT transaction_id, item_index
              FROM sp_transaction_items
              GROUP BY transaction_id, item_index
              HAVING COUNT(*) > 1
            ) s;
        """)
        results["duplicate_items"] = cur.fetchone()[0]

        # Orphan items (should not exist because of FK)
        cur.execute("""
            SELECT COUNT(*)
            FROM sp_transaction_items i
            LEFT JOIN sp_transactions t
                   ON t.transaction_id = i.transaction_id
            WHERE t.transaction_id IS NULL;
        """)
        results["orphan_items"] = cur.fetchone()[0]

    LOG.info("VALIDATION: %s", results)
    return results


def write_validation_csv(path: str, metrics: dict):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["metric", "value"])
        for k, v in metrics.items():
            w.writerow([k, v])


# -------------------- CLI --------------------

def parse_args():
    ap = argparse.ArgumentParser(description="SP-API Finances → Postgres")
    ap.add_argument("--posted-after", default=env("POSTED_AFTER", required=True))
    ap.add_argument("--posted-before", default=env("POSTED_BEFORE") or None)
    ap.add_argument("--marketplace-id", default=env("MARKETPLACE_ID") or None)
    ap.add_argument("--transaction-status", default=None)
    ap.add_argument("--host", default=env("SPAPI_HOST", "sellingpartnerapi-na.amazon.com"))
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--summary-csv", default=None)
    return ap.parse_args()


def main():
    args = parse_args()
    mock = os.getenv("MOCK_MODE", "0") in ("1", "true", "TRUE")
    auth = None if mock else get_awsauth()

    conn = db_connect()
    init_db(conn)

    lister = list_transactions_mock if mock else list_transactions

    count = 0
    for tx in lister(
        args.posted_after, args.posted_before, args.marketplace_id, args.transaction_status, args.host, auth
    ):
        try:
            upsert_transaction(conn, tx)
        except psycopg.OperationalError:
            LOG.warning("Lost connection to Postgres. Retrying with new connection…")
            try:
                conn.close()
            except Exception:
                pass
            conn = db_connect()
            init_db(conn)
            upsert_transaction(conn, tx)

        count += 1
        if args.limit and count >= args.limit:
            break

    LOG.info("Ingestion completed. Transactions processed: %s", count)

    summary = summarize_by_sku(conn)
    for sku, total in summary[:20]:
        LOG.info("  %s: %.2f", sku, total)

    if args.summary_csv:
        write_summary_csv(args.summary_csv, summary)
        LOG.info("Summary CSV written to: %s", args.summary_csv)

    # ---- Explicit validations (missing/duplicates) ----
    val = run_validations(conn)

    # Optional export of validation report to CSV (if VALIDATION_CSV is set)
    validation_csv_path = os.getenv("VALIDATION_CSV")
    if validation_csv_path:
        write_validation_csv(validation_csv_path, val)
        LOG.info("Validation CSV written to: %s", validation_csv_path)


if __name__ == "__main__":
    main()
