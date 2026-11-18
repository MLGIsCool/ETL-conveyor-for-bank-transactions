# ETL-конвейер для агрегации и анализа финансовых транзакций мобильного банка

## Описание проекта

Автоматизированный ETL-конвейер для сбора, обработки и загрузки данных о финансовых транзакциях мобильного банка в целевое аналитическое хранилище с обеспечением контроля качества данных.

Проект реализует полный цикл обработки данных: от извлечения из CSV-файла до загрузки в PostgreSQL с промежуточной очисткой, валидацией и нормализацией.

## Технологический стек

- **Язык программирования**: Python 3.12
- **Обработка данных**: Pandas 2.0+, scikit-learn 1.4+
- **Контроль качества**: Great Expectations 1.2+
- **Оркестрация**: Apache Airflow 2.10.5
- **Хранилище данных (DWH)**: PostgreSQL 17
- **Data Lake**: SeaweedFS (S3-совместимое хранилище)
- **Контейнеризация**: Docker + Docker Compose
- **Тестирование**: pytest 8.0+

## Архитектура

Конвейер реализует классический ETL-подход:

1. **Extract**: Извлечение данных из CSV-файла с поддержкой полной и инкрементальной загрузки
2. **Transform**: Очистка, валидация, нормализация и обогащение данных
3. **Load**: Загрузка в DWH (PostgreSQL) и Data Lake (S3)
4. **Quality**: Контроль качества данных через Great Expectations

### Схема данных

Реализована схема "звезда":
- **Факт-таблица**: `fact_transactions` - центральная таблица транзакций
- **Таблицы измерений**: `dim_customers`, `dim_channels`, `dim_devices`, `dim_merchants`, `dim_locations`
- **Витрина**: `daily_channel_stats` - агрегированная статистика по дням и каналам

## Структура проекта

bank_transactions_etl/

├── docker-compose.yml # Описание контейнеров и сервисов

├── Dockerfile # Образ для Airflow

├── .env # Переменные окружения

├── requirements.txt # Python-зависимости

├── README.md # Документация проекта

│

├── dags/

│ └── bank_etl_dag.py # DAG для Airflow

│

├── src/

│ ├── extract.py # Модуль извлечения данных

│ ├── transform.py # Модуль трансформации

│ ├── load.py # Модуль загрузки

│ └── quality.py # Проверки качества (Great Expectations)

│

├── config/

│ └── great_expectations.py # Бизнес-правила валидации

│

├── db/

│ └── init.sql # Скрипт инициализации схемы DWH

│

├── tests/ # Модульные тесты

│

└── data/

├── raw/ # Исходные CSV-файлы

│ └── bank_transactions.csv

└── staging/ # Промежуточные файлы (создаётся автоматически)

## Требования

- Docker Desktop / Docker Engine
- Docker Compose
- Минимум 8 ГБ оперативной памяти
- Свободное место на диске: ~2 ГБ

## Установка и запуск

### 1. Клонирование проекта
```bash
git clone <repository-url>
cd bank_transactions_etl
```
### 2. Подготовка данных
Поместите CSV-файл в директорию data/raw/:
```bash
cp /path/to/your/bank_transactions.csv data/raw/
```
Формат CSV: файл должен содержать колонки:
TransactionID, AccountID, TransactionAmount, TransactionDate
TransactionType, Channel, Location, CustomerAge
CustomerOccupation, LoginAttempts, AccountBalance
DeviceID, MerchantID, IPAddress, TransactionDuration

### 3. Создание необходимых директорий
```bash
mkdir -p logs data/staging
chmod -R 777 logs data
```
### 4. Запуск инфраструктуры
```bash
docker compose up -d --build
```
Эта команда запустит:
PostgreSQL (порт 5432)
SeaweedFS S3 (порт 8333)
Airflow Webserver (порт 8080)
Airflow Scheduler
5. Проверка статуса
```bash
docker compose ps
```
Все сервисы должны быть в состоянии Up (healthy).

### Использование
### Запуск DAG
Через веб-интерфейс Airflow:
Откройте http://localhost:8080
Войдите с учётными данными: admin / admin
Найдите DAG bank_etl
Нажмите кнопку Trigger DAG

### Мониторинг выполнения
Airflow UI: http://localhost:8080 - визуализация DAG, логи задач
Логи: docker compose logs -f airflow-scheduler

### Проверка результатов
### Проверка DWH (PostgreSQL):
```bash
# Подключение к БД
docker compose exec postgres psql -U bank -d bank_dwh
# Счётчик транзакций
SELECT COUNT(*) FROM dwh.fact_transactions;
# Проверка витрины
SELECT * FROM dwh.daily_channel_stats ORDER BY stat_date DESC LIMIT 10;
# Выход
\q
```
### Проверка Data Lake (S3):
```bash
docker compose exec airflow-scheduler python3 -c "
import os, boto3
s3 = boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'], 
                  aws_access_key_id=os.environ['S3_ACCESS_KEY'], 
                  aws_secret_access_key=os.environ['S3_SECRET_KEY'])
resp = s3.list_objects_v2(Bucket=os.environ['S3_BUCKET'])
for obj in resp.get('Contents', []):
    print(obj['Key'])
"
```
### Инкрементальная загрузка
При повторных запусках конвейер обрабатывает только новые транзакции (позже максимальной даты в DWH). Это реализовано через механизм watermark.
### Полная перезагрузка
Для принудительной полной загрузки установите переменную окружения:
```bash
FORCE_FULL_LOAD=true docker compose exec airflow-scheduler airflow dags trigger bank_etl
```
### Тестирование
### Запуск модульных тестов
```bash
docker compose exec airflow-scheduler pytest /opt/airflow/tests -v
```
### Проверка качества данных
Конвейер автоматически выполняет проверки через Great Expectations:
Полнота данных (отсутствие пропусков в ключевых полях)
Уникальность идентификаторов
Соответствие бизнес-правилам (сумма > 0, допустимые значения типов)
При нарушении проверок пайплайн останавливается до этапа загрузки.
### Остановка проекта
```bash
# Остановка контейнеров
docker compose down
# Остановка с удалением томов (полная очистка)
docker compose down -v
```
### Переменные окружения
Основные параметры настраиваются через файл .env:

Переменная      Описание              По умолчанию

POSTGRES_USER  Пользователь  PostgreSQL   bank

POSTGRES_PASSWORD  Пароль PostgreSQL    bank

POSTGRES_DB   Имя базы данных   bank_dwh

S3_ENDPOINT   Endpoint S3-хранилища   http://s3:8333

S3_ACCESS_KEY   Ключ доступа S3   dev

S3_SECRET_KEY   Секретный ключ S3   dev

S3_BUCKET   Имя бакета S3   bank-datalake

### Особенности реализации
### Обработка ошибок

Retry-политика: Airflow автоматически перезапускает задачи при сбоях (2 попытки с интервалом 1 минута)

Dead Letter Queue (DLQ): невалидные строки сохраняются в S3 для последующего анализа

Логирование: все этапы логируются с уровнями INFO, WARNING, ERROR

### Идемпотентность

Многократный запуск на одних и тех же данных не создаёт дубликатов благодаря:

UPSERT-операциям в PostgreSQL (INSERT ... ON CONFLICT DO UPDATE)

Идемпотентному пересчёту витрин

### Масштабируемость

Архитектура позволяет:

Обрабатывать до 1 000 000 транзакций за один запуск

Легко добавлять новые источники данных

Переходить на распределённую обработку (PySpark) без изменения кода

Лицензия

Учебный проект, разработан в рамках курсовой работы по дисциплине "Инжиниринг данных".

