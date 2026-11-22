import os
from contextlib import closing

import pandas as pd
import psycopg2
import pytest

from src.extract import run as extract_run
from src.transform import run as transform_run
from src.load import run as load_run

def _count(dsn, sql):
    with closing(psycopg2.connect(dsn)) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]


def test_full_etl_cycle(clean_dwh, set_data_dir, sample_csv):
    ex = extract_run()
    assert ex["rows"] == 4           
    assert os.path.exists(ex["path"])

    tr = transform_run(ex["path"])
    assert tr["rows"] == 2                    
    assert tr["dlq_rows"] == 1                
    assert os.path.exists(tr["fact"])

    load_run(tr, ds="2026-11-18")

    assert _count(clean_dwh, "SELECT COUNT(*) FROM dwh.fact_transactions") == 2
    assert _count(clean_dwh, "SELECT COUNT(*) FROM dwh.daily_channel_stats") == 2


def test_idempotency(clean_dwh, set_data_dir, sample_csv):
    """NFR-3: повторный Load не плодит дубликаты ни в факте, ни в витрине."""
    tr = transform_run(extract_run()["path"])

    load_run(tr, ds="2026-11-18")
    c1 = _count(clean_dwh, "SELECT COUNT(*) FROM dwh.fact_transactions")
    m1 = _count(clean_dwh, "SELECT COUNT(*) FROM dwh.daily_channel_stats")

    load_run(tr, ds="2026-11-18")
    c2 = _count(clean_dwh, "SELECT COUNT(*) FROM dwh.fact_transactions")
    m2 = _count(clean_dwh, "SELECT COUNT(*) FROM dwh.daily_channel_stats")

    assert c1 == c2 == 2, f"UPSERT не идемпотентен: {c1} → {c2}"
    assert m1 == m2 == 2, f"Витрина дублируется: {m1} → {m2}"


def test_quality_blocks_bad_data(clean_dwh, set_data_dir, tmp_path):
    """FR-5.2: невалидная строка уходит в DLQ и не доходит до факта."""
    bad = pd.DataFrame([{
        "TransactionID": "BAD1", "AccountID": "A1", "TransactionAmount": 0.0,
        "TransactionDate": "4/11/2023 16:29", "TransactionType": "Debit",
        "Channel": "ATM", "Location": "Moscow", "CustomerAge": 30,
        "CustomerOccupation": "Doctor", "LoginAttempts": 1,
        "AccountBalance": 5000.0, "DeviceID": "D1", "MerchantID": "M1",
        "IP Address": "1.1.1.1", "TransactionDuration": 30,
    }])
    p = tmp_path / "raw" / "bank_transactions.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    bad.to_csv(p, index=False)

    tr = transform_run(extract_run()["path"])
    assert tr["dlq_rows"] == 1
    assert tr["rows"] == 0