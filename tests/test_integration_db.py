import os, csv, pathlib
import psycopg
import pytest
import main as m

PG_DSN = os.getenv("PG_DSN", "postgresql://app:app@localhost:5432/spapi")

@pytest.fixture(scope="session")
def conn():
    # Asegurate de tener `docker compose up -d db`
    c = psycopg.connect(PG_DSN)
    m.init_db(c)
    yield c
    c.close()

@pytest.fixture(autouse=True)
def clean(conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE sp_transaction_items RESTART IDENTITY CASCADE")
        cur.execute("TRUNCATE sp_transactions RESTART IDENTITY CASCADE")
    conn.commit()
    yield

@pytest.mark.integration
def test_upsert_y_summary_en_db(conn, tmp_path: pathlib.Path):
    tx1 = {
        "transactionId": "T-1",
        "postedDate": "2025-08-01T00:00:00Z",
        "transactionType": "Charge",
        "transactionStatus": "RELEASED",
        "marketplaceDetails": {"marketplaceId": "ATVPDKIKX0DER", "marketplaceName": "Amazon.com"},
        "totalAmount": {"currencyCode": "USD", "currencyAmount": 10.00},
        "contexts": [{"sku": "SKU-AAA"}],
        "items": [{"description": "I1","totalAmount":{"currencyCode":"USD","currencyAmount":10.00},"contexts":[{"sku":"SKU-AAA"}]}],
    }
    tx2 = {
        "transactionId": "T-2",
        "postedDate": "2025-08-01T01:00:00Z",
        "transactionType": "Charge",
        "transactionStatus": "RELEASED",
        "marketplaceDetails": {"marketplaceId": "ATVPDKIKX0DER", "marketplaceName": "Amazon.com"},
        "totalAmount": {"currencyCode": "USD", "currencyAmount": 20.00},
        "items": [{"description": "I2","totalAmount":{"currencyCode":"USD","currencyAmount":20.00},"contexts":[{"sku":"SKU-AAA"}]}],
    }

    m.upsert_transaction(conn, tx1)
    m.upsert_transaction(conn, tx2)

    summary = dict(m.summarize_by_sku(conn))
    assert summary.get("SKU-AAA") == 30.0

    # Export CSV y validar formato
    csv_path = tmp_path / "summary.csv"
    m.write_summary_csv(str(csv_path), list(summary.items()))
    with open(csv_path) as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == ["sku", "total_amount"]
    assert rows[1] == ["SKU-AAA", "30.0"]
