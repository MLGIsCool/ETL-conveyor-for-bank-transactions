import os
from contextlib import closing
import psycopg2
import pytest
import pandas as pd

TABLES = ("dwh.fact_transactions", "dwh.daily_channel_stats", "dwh.dim_customers",
          "dwh.dim_channels", "dwh.dim_devices", "dwh.dim_merchants", "dwh.dim_locations")


@pytest.fixture
def dwh_dsn():
    return os.environ.get("DWH_DSN", "postgresql://bank:bank@postgres:5432/bank_dwh_test")

@pytest.fixture
def clean_dwh(dwh_dsn, monkeypatch):
    monkeypatch.setenv("DWH_DSN", dwh_dsn)
    with closing(psycopg2.connect(dwh_dsn)) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE " + ", ".join(TABLES))
        conn.commit()
    return dwh_dsn

@pytest.fixture
def sample_csv(tmp_path):
    """Создаёт небольшой CSV с известными данными для тестов."""
    data = [
        {"TransactionID": "T1", "AccountID": "A1", "TransactionAmount": 100.0,
         "TransactionDate": "4/11/2023 16:29", "TransactionType": "Debit",
         "Channel": "ATM", "Location": "Moscow", "CustomerAge": 30,
         "CustomerOccupation": "Doctor", "LoginAttempts": 1,
         "AccountBalance": 5000.0, "DeviceID": "D1", "MerchantID": "M1",
         "IP Address": "1.1.1.1", "TransactionDuration": 30},
        {"TransactionID": "T2", "AccountID": "A2", "TransactionAmount": 200.0,
         "TransactionDate": "4/12/2023 10:00", "TransactionType": "Credit",
         "Channel": "Online", "Location": None, "CustomerAge": 25,
         "CustomerOccupation": None, "LoginAttempts": 2,
         "AccountBalance": 3000.0, "DeviceID": "D2", "MerchantID": "M2",
         "IP Address": None, "TransactionDuration": 60},
        {"TransactionID": "T1", "AccountID": "A1", "TransactionAmount": 100.0,
         "TransactionDate": "4/11/2023 16:29", "TransactionType": "Debit",
         "Channel": "ATM", "Location": "Moscow", "CustomerAge": 30,
         "CustomerOccupation": "Doctor", "LoginAttempts": 1,
         "AccountBalance": 5000.0, "DeviceID": "D1", "MerchantID": "M1",
         "IP Address": "1.1.1.1", "TransactionDuration": 30},
        {"TransactionID": "T3", "AccountID": "A3", "TransactionAmount": -50.0,
         "TransactionDate": "4/13/2023 12:00", "TransactionType": "Debit",
         "Channel": "ATM", "Location": "SPb", "CustomerAge": 40,
         "CustomerOccupation": "Engineer", "LoginAttempts": 1,
         "AccountBalance": 1000.0, "DeviceID": "D3", "MerchantID": "M3",
         "IP Address": "2.2.2.2", "TransactionDuration": 20},
    ]
    df = pd.DataFrame(data)
    csv_path = tmp_path / "raw" / "bank_transactions.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    return csv_path

@pytest.fixture
def staging_dir(tmp_path):
    # Возвращает путь к staging-директории
    d = tmp_path / "staging"
    d.mkdir(parents=True, exist_ok=True)
    return d

@pytest.fixture
def set_data_dir(monkeypatch, tmp_path):
    # Переопределяет DATA_DIR на tmp_path для тестов
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    return tmp_path