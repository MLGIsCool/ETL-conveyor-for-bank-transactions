from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging
import sys
import os

# Добавляем путь к модулям проекта
sys.path.insert(0, '/opt/airflow/src')

from src.extract.extract import extract
from src.transform.transform import normalize, clean
from src.load.load import load_to_dwh, save_dlq_to_s3
from src.quality.expectations import run_quality_check

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Параметры DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # NFR-4: автоматический перезапуск при сбое
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1),
}


def extract_task(**context):
    """Задача Extract: извлечение данных из CSV."""
    logger.info("Начало задачи Extract")
    csv_path = '/opt/airflow/data/raw/transactions.csv'

    # Проверяем существование файла
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Файл {csv_path} не найден")

    df = extract(csv_path)

    if df.empty:
        logger.warning("Нет новых данных для обработки (инкрементальная загрузка)")
        context['ti'].xcom_push(key='skip_downstream', value=True)
        return

    context['ti'].xcom_push(key='raw_data', value=df.to_json())
    context['ti'].xcom_push(key='row_count', value=len(df))
    logger.info(f"Extract завершён. Извлечено строк: {len(df)}")


def quality_check_task(**context):
    """Задача Data Quality Check: валидация данных через Great Expectations."""
    skip = context['ti'].xcom_pull(key='skip_downstream')
    if skip:
        logger.info("Пропускаем Quality Check (нет данных)")
        return

    raw_json = context['ti'].xcom_pull(key='raw_data')
    import pandas as pd
    df = pd.read_json(raw_json)

    logger.info("Начало проверки качества данных")
    is_valid = run_quality_check(df)

    if not is_valid:
        # FR-5.2: остановка пайплайна при критических ошибках
        raise ValueError("Данные не прошли проверку качества. Пайплайн остановлен.")

    logger.info("Данные прошли проверку качества")


def transform_task(**context):
    """Задача Transform: очистка и нормализация данных."""
    skip = context['ti'].xcom_pull(key='skip_downstream')
    if skip:
        logger.info("⏭Пропускаем Transform (нет данных)")
        return

    raw_json = context['ti'].xcom_pull(key='raw_data')
    import pandas as pd
    df = pd.read_json(raw_json)

    logger.info("Начало трансформации данных")

    # 1. Очистка данных
    df_clean, dlq_df = clean(df)
    logger.info(f"Очистка: {len(df_clean)} чистых строк, {len(dlq_df)} отбраковано")

    # 2. Нормализация (разделение на факты и измерения)
    normalized = normalize(df_clean)
    logger.info(
        f"   Нормализация: "
        f"dim_channel={len(normalized['dim_channel'])}, "
        f"dim_device={len(normalized['dim_device'])}, "
        f"dim_location={len(normalized['dim_location'])}, "
        f"fact_transactions={len(normalized['fact_transactions'])}"
    )

    # Сохраняем результаты через XCom (в виде JSON)
    context['ti'].xcom_push(key='normalized_data', value={
        k: v.to_json() for k, v in normalized.items()
    })
    context['ti'].xcom_push(key='dlq_data', value=dlq_df.to_json())

    logger.info("Transform завершён")


def load_task(**context):
    """Задача Load: загрузка данных в DWH и S3."""
    skip = context['ti'].xcom_pull(key='skip_downstream')
    if skip:
        logger.info("Пропускаем Load (нет данных)")
        return

    import pandas as pd
    import json

    normalized_json = context['ti'].xcom_pull(key='normalized_data')
    dlq_json = context['ti'].xcom_pull(key='dlq_data')

    # Восстанавливаем DataFrame'ы
    normalized = {k: pd.read_json(v) for k, v in normalized_json.items()}
    dlq_df = pd.read_json(dlq_json)

    logger.info("Начало загрузки данных")

    # 1. Загрузка в PostgreSQL (DWH)
    load_to_dwh(normalized)

    # 2. Сохранение отбракованных записей в Arenadata S3 (DLQ)
    if not dlq_df.empty:
        save_dlq_to_s3(dlq_df)

    logger.info("Load завершён. Данные успешно загружены в DWH и S3.")


# Определение DAG
with DAG(
    dag_id='etl_bank_transactions',
    default_args=default_args,
    description='ETL-конвейер для агрегации финансовых транзакций мобильного банка',
    schedule_interval='@daily',  # Запуск ежедневно (FR-4.1)
    catchup=False,
    tags=['etl', 'banking', 'finance'],
) as dag:

    # Стартовый и конечный операторы для визуализации
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Определение задач
    extract_op = PythonOperator(
        task_id='extract_data',
        python_callable=extract_task,
    )

    quality_op = PythonOperator(
        task_id='quality_check',
        python_callable=quality_check_task,
    )

    transform_op = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task,
    )

    load_op = PythonOperator(
        task_id='load_data',
        python_callable=load_task,
    )

    # Определение зависимостей (FR-4.2)
    # Extract → Quality Check → Transform → Load
    start >> extract_op >> quality_op >> transform_op >> load_op >> end