import os
import pandas as pd
import psycopg2

def run():
    # 1. Читаем CSV (используем DATA_DIR из env для тестов, иначе дефолт)
    data_dir = os.getenv("DATA_DIR", "/opt/airflow/data")
    src_path = os.path.join(data_dir, "raw", "bank_transactions.csv")
    out_path = os.path.join(data_dir, "staging", "extracted.csv")
    
    df = pd.read_csv(src_path, encoding="utf-8-sig")
    s = df["TransactionDate"]
    d = pd.to_datetime(s, format="%m/%d/%Y %H:%M", errors="coerce")
    miss = d.isna()
    if miss.any():
        d.loc[miss] = pd.to_datetime(s[miss], format="mixed", errors="coerce")
    df["TransactionDate"] = d

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
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)
    return {"path": out_path, "rows": len(df)}