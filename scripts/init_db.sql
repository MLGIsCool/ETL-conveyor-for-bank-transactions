-- Создание таблицы фактов (транзакции)
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50),
    amount NUMERIC(15, 2),
    transaction_date TIMESTAMP,
    status VARCHAR(20)
);

-- Создание таблицы измерений (каналы)
CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id SERIAL PRIMARY KEY,
    channel_name VARCHAR(50) UNIQUE NOT NULL
);