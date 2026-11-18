import os
import pandas as pd
import psycopg2

def run():
    # 1. Читаем CSV
    src_path = "/opt/airflow/data/raw/bank_transactions.csv"
    out_path = "/opt/airflow/data/staging/extracted.csv"
    
    df = pd.read_csv("/opt/airflow/data/raw/bank_transactions.csv")
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"], errors="coerce", format="mixed")

    # 2. Узнаём последнюю загруженную дату из DWH
    conn = psycopg2.connect(os.environ["DWH_DSN"])
    cur = conn.cursor()
    cur.execute("SELECT MAX(transaction_date) FROM dwh.fact_transactions")
    last_date = cur.fetchone()[0]
    conn.close()

    # 3. Если данные уже грузились — берём только новые строки
    if last_date is not None:
        df = df[df["TransactionDate"] > last_date]

    # 4. Сохраняем для Transform
    os.makedirs("/opt/airflow/data/staging", exist_ok=True)
    df.to_csv("/opt/airflow/data/staging/extracted.csv", index=False)
    return {"path": out_path, "rows": len(df)}