import os
import logging
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import boto3
from botocore.client import Config
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


def get_db_connection():
    """Устанавливает соединение с PostgreSQL DWH."""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5433'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )


def get_s3_client():
    """Инициализирует клиент для Arenadata S3 (или локального S3-эмулятора)."""
    return boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT'),
        aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def upsert_dataframe(conn, df: pd.DataFrame, table_name: str, conflict_key: str, columns: list):
    """
    Выполняет UPSERT DataFrame в таблицу PostgreSQL.

    Args:
        conn: Соединение с PostgreSQL
        df: DataFrame для загрузки
        table_name: Имя целевой таблицы
        conflict_key: Колонка для проверки конфликта (ON CONFLICT)
        columns: Список колонок для вставки
    """
    if df.empty:
        logger.info(f"Таблица {table_name} пуста, загрузка пропущена")
        return 0

    # Формируем SQL-запрос с UPSERT
    cols = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != conflict_key])

    sql = f"""
        INSERT INTO {table_name} ({cols})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_key}) DO UPDATE SET {update_clause}
    """

    # Преобразуем DataFrame в список кортежей
    records = [tuple(row) for row in df[columns].itertuples(index=False, name=None)]

    with conn.cursor() as cur:
        execute_values(cur, sql, records)

    logger.info(f"Загружено в {table_name}: {len(records)} строк (UPSERT)")
    return len(records)


def load_to_dwh(data_dict: dict[str, pd.DataFrame]) -> None:
    conn = None
    try:
        conn = get_db_connection()

        # Загрузка в рамках одной транзакции (атомарность)
        with conn.cursor() as cur:
            cur.execute("BEGIN;")

        # 1. Загрузка таблиц измерений
        dim_mappings = {
            'dim_channel': ('channel_id', ['channel_id', 'channel_name']),
            'dim_device': ('device_id', ['device_id', 'device_name']),
            'dim_location': ('location_id', ['location_id', 'location_name']),
        }

        # Переименование колонок для соответствия схеме БД
        data_dict['dim_channel'] = data_dict['dim_channel'].rename(columns={'Channel': 'channel_name'})
        data_dict['dim_device'] = data_dict['dim_device'].rename(columns={'DeviceID': 'device_name'})
        data_dict['dim_location'] = data_dict['dim_location'].rename(columns={'Location': 'location_name'})

        for table_name, (conflict_key, columns) in dim_mappings.items():
            if table_name in data_dict:
                upsert_dataframe(conn, data_dict[table_name], table_name, conflict_key, columns)

        # 2. Загрузка таблицы фактов
        fact_columns = [
            'TransactionID', 'AccountID', 'channel_id', 'device_id', 'location_id',
            'TransactionAmount', 'TransactionType', 'TransactionDate',
            'AccountBalance', 'transaction_hour'
        ]
        upsert_dataframe(
            conn,
            data_dict['fact_transactions'],
            'fact_transactions',
            'TransactionID',
            fact_columns
        )

        conn.commit()
        logger.info("Все данные успешно загружены в DWH")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка загрузки в DWH: {e}")
        raise
    finally:
        if conn:
            conn.close()


def save_dlq_to_s3(dlq_df: pd.DataFrame) -> None:
    """Сохраняет отбракованные записи в Arenadata S3 (Data Lake)."""
    if dlq_df.empty:
        logger.info("DLQ пуста, загрузка в S3 пропущена")
        return

    try:
        s3 = get_s3_client()
        bucket = os.getenv('S3_BUCKET_DLQ')

        # Формируем имя файла с временной меткой
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        key = f"dlq_{timestamp}.parquet"

        # Сериализация в Parquet (эффективный формат для S3)
        parquet_buffer = dlq_df.to_parquet(index=False)

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_buffer
        )
        logger.info(f"DLQ сохранена в S3: s3://{bucket}/{key} ({len(dlq_df)} записей)")

    except Exception as e:
        logger.error(f"Ошибка сохранения DLQ в S3: {e}")
        raise