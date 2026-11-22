import logging
import os
import time

import boto3
import pandas as pd
import psycopg2
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# части по 16 МБ вместо 8 МБ, 4 потока — меньше запросов, умеренный расход памяти
S3_CFG = TransferConfig(multipart_threshold=16 * 1024 * 1024,
                        multipart_chunksize=16 * 1024 * 1024,
                        max_concurrency=4)


def _upsert(cur, table: str, df: pd.DataFrame, pk: list) -> None:
    if df.empty:
        return
    df = df.astype(object).where(pd.notna(df), None)
    cols = list(df.columns)
    updates = [c for c in cols if c not in pk]
    action = ("DO UPDATE SET " + ", ".join(f"{c}=EXCLUDED.{c}" for c in updates)) if updates else "DO NOTHING"
    sql = (f"INSERT INTO dwh.{table} ({', '.join(cols)}) VALUES %s "
           f"ON CONFLICT ({', '.join(pk)}) {action}")
    execute_values(cur, sql, list(df.itertuples(index=False, name=None)), page_size=1000)


def run(tr: dict, ds: str) -> None:
    t0 = time.time()

    # 1. Data Lake (S3): архив исходника, DLQ, обработанные признаки
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                      aws_access_key_id=os.environ["S3_ACCESS_KEY"],
                      aws_secret_access_key=os.environ["S3_SECRET_KEY"])
    bucket = os.environ["S3_BUCKET"]
    try:
        s3.create_bucket(Bucket=bucket)
    except ClientError:
        pass
    raw = os.path.join(os.getenv("DATA_DIR", "/opt/airflow/data"), "raw", "bank_transactions.csv")
    s3.upload_file(raw, bucket, f"raw/{ds}_bank_transactions.csv", Config=S3_CFG)
    if tr.get("dlq"):
        s3.upload_file(tr["dlq"], bucket, f"dlq/{ds}_dlq.csv", Config=S3_CFG)
    if tr.get("features"):
        ext = os.path.splitext(tr["features"])[1]         # .csv или .parquet
        s3.upload_file(tr["features"], bucket, f"processed/{ds}_features{ext}", Config=S3_CFG)
    t_s3 = time.time() - t0

    # 2. DWH: сначала измерения, затем факты (UPSERT), затем витрина
    t1 = time.time()
    with psycopg2.connect(os.environ["DWH_DSN"]) as conn, conn.cursor() as cur:
        for name, path in tr["dims"].items():
            d = pd.read_csv(path)
            _upsert(cur, name, d, pk=[d.columns[0]])
        fact = pd.read_csv(tr["fact"], parse_dates=["transaction_date"]) if tr.get("fact") else pd.DataFrame()
        _upsert(cur, "fact_transactions", fact, pk=["transaction_id"])

        if tr["dates"]:
            cur.execute("DELETE FROM dwh.daily_channel_stats WHERE stat_date = ANY(%s::date[])", (tr["dates"],))
            cur.execute("""
                INSERT INTO dwh.daily_channel_stats (stat_date, channel, tx_count, total_amount, avg_amount)
                SELECT transaction_date::date, channel, COUNT(*), SUM(amount), AVG(amount)
                FROM dwh.fact_transactions
                WHERE transaction_date::date = ANY(%s::date[])
                GROUP BY 1, 2
            """, (tr["dates"],))
    t_db = time.time() - t1

    log.info("Load: фактов=%d, DLQ=%d, S3=%.1fs, DWH=%.1fs",
             tr["rows"], tr["dlq_rows"], t_s3, t_db)