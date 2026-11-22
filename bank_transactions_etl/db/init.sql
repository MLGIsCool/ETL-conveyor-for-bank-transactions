CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.dim_customers (
    account_id TEXT PRIMARY KEY, customer_age INT, customer_occupation TEXT);
CREATE TABLE IF NOT EXISTS dwh.dim_channels  (channel TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS dwh.dim_devices   (device_id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS dwh.dim_merchants (merchant_id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS dwh.dim_locations (location TEXT PRIMARY KEY);

CREATE TABLE IF NOT EXISTS dwh.fact_transactions (
    transaction_id       TEXT PRIMARY KEY,
    account_id           TEXT REFERENCES dwh.dim_customers(account_id),
    channel              TEXT REFERENCES dwh.dim_channels(channel),
    device_id            TEXT REFERENCES dwh.dim_devices(device_id),
    merchant_id          TEXT REFERENCES dwh.dim_merchants(merchant_id),
    location             TEXT REFERENCES dwh.dim_locations(location),
    transaction_date     TIMESTAMP NOT NULL,
    amount               NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    transaction_type     TEXT,
    login_attempts       INT,
    account_balance      NUMERIC(14,2),
    transaction_duration INT,
    ip_address           TEXT,
    loaded_at            TIMESTAMP DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fact_date ON dwh.fact_transactions(transaction_date);

CREATE TABLE IF NOT EXISTS dwh.daily_channel_stats (
    stat_date DATE, channel TEXT,
    tx_count INT, total_amount NUMERIC(14,2), avg_amount NUMERIC(14,2),
    PRIMARY KEY (stat_date, channel));