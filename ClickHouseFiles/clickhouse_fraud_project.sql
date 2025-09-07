
-- ================================================
-- ClickHouse Schema for Fraud Detection Project
-- ================================================

-- Source table: transactions_realtime (already written by Spark)
-- Example schema (adjust based on your actual ingestion job)
CREATE TABLE IF NOT EXISTS transactions_realtime
(
    transaction_id UInt64,
    customer_id UInt32,
    amount Float64,
    prediction UInt8,
    event_time DateTime
)
ENGINE = MergeTree()
ORDER BY event_time;

-- ================================================
-- Job 2 – Real-time Aggregations
-- ================================================

-- Fraud Rate Daily
CREATE TABLE IF NOT EXISTS fraud_daily
(
    event_date Date,
    fraud_count UInt64,
    total_count UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fraud_daily
TO fraud_daily
AS
SELECT
    toDate(event_time) AS event_date,
    countIf(prediction = 1) AS fraud_count,
    count() AS total_count
FROM transactions_realtime
GROUP BY event_date;

-- Fraud Rate Hourly
CREATE TABLE IF NOT EXISTS fraud_hourly
(
    event_hour DateTime,
    fraud_count UInt64,
    total_count UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(event_hour)
ORDER BY event_hour;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fraud_hourly
TO fraud_hourly
AS
SELECT
    toStartOfHour(event_time) AS event_hour,
    countIf(prediction = 1) AS fraud_count,
    count() AS total_count
FROM transactions_realtime
GROUP BY event_hour;

-- Top Fraud Customers
CREATE TABLE IF NOT EXISTS top_fraud_customers
(
    customer_id UInt64,
    fraud_transactions UInt64
)
ENGINE = SummingMergeTree()
ORDER BY customer_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_fraud_customers
TO top_fraud_customers
AS
SELECT
    customer_id,
    countIf(prediction = 1) AS fraud_transactions
FROM transactions_realtime
GROUP BY customer_id;

-- ================================================
-- Job 3 – Batch Aggregations (Historical)
-- ================================================

-- Transactions Daily Summary
CREATE TABLE IF NOT EXISTS transactions_daily
(
    event_date Date,
    total_amount Float64,
    avg_amount Float64,
    transaction_count UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_transactions_daily
TO transactions_daily
AS
SELECT
    toDate(event_time) AS event_date,
    sum(amount) AS total_amount,
    avg(amount) AS avg_amount,
    count() AS transaction_count
FROM transactions_realtime
GROUP BY event_date;
