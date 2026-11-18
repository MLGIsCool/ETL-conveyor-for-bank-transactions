import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Настройка логирования (NFR-8)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения из файла .env
load_dotenv()


def get_db_connection():
        return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5433'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

def get_max_transaction_date():
    conn = None
    max_date = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT MAX(transaction_date) AS max_date FROM fact_transactions;")
            result = cur.fetchone()
            if result and result['max_date']:
                max_date = result['max_date']
                logger.info(f"Последняя загруженная дата: {max_date}")
            else:
                logger.info("Таблица fact_transactions пуста. Будет выполнена полная загрузка.")
    except Exception as e:
        logger.error(f"Ошибка при получении максимальной даты: {e}")
        raise
    finally:
        if conn:
            conn.close()
    return max_date


def extract(csv_path: str) -> pd.DataFrame:
    """
    Извлекает данные из CSV-файла с поддержкой инкрементальной загрузки.
    Returns:
        pd.DataFrame: DataFrame с отфильтрованными данными для обработки
    """
    logger.info(f"Начало извлечения данных из файла: {csv_path}")

    # 1. Чтение CSV-файла
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Прочитано строк из источника: {len(df)}")
    except Exception as e:
        logger.error(f"Ошибка чтения CSV-файла: {e}")
        raise

    # 2. Получение границы инкремента из DWH
    max_date = get_max_transaction_date()

    # 3. Конвертация дат для корректного сравнения
    df['TransactionDate'] = pd.to_datetime(df['TransactionDate'], errors='coerce')

    # 4. Фильтрация для инкрементальной загрузки
    if max_date is not None:
        df_filtered = df[df['TransactionDate'] > max_date]
        logger.info(f"После инкрементальной фильтрации осталось строк: {len(df_filtered)}")
    else:
        df_filtered = df
        logger.info("Режим полной загрузки (Full Load)")

    return df_filtered