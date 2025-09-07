
-- ==================================================
-- ClickHouse AggregatingMergeTree Example
-- For Fraud Detection Project - Daily Transactions
-- ==================================================
-- This schema demonstrates how to use AggregatingMergeTree
-- for storing aggregated metrics like SUM, COUNT, and AVG natively.
-- Unlike SummingMergeTree, it supports complex aggregates
-- by storing aggregate states (sumState, avgState, etc.)
-- and merging them efficiently during queries.
-- ==================================================

-- Target table with aggregate function states
CREATE TABLE IF NOT EXISTS transactions_daily
(
    event_date Date,
    sum_state AggregateFunction(sum, Float64),
    count_state AggregateFunction(count, UInt64),
    avg_state AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

-- Materialized View to populate the table from realtime transactions
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_transactions_daily
TO transactions_daily
AS
SELECT
    toDate(event_time) AS event_date,
    sumState(amount) AS sum_state,
    countState() AS count_state,
    avgState(amount) AS avg_state
FROM transactions_realtime
GROUP BY event_date;

-- ==================================================
-- Example query for consumption (e.g., Power BI, Tableau)
-- ==================================================
-- This query merges the aggregate states into final results.
-- It returns total amount, transaction count, and average amount per day.
SELECT
    event_date,
    sumMerge(sum_state) AS total_amount,
    countMerge(count_state) AS transaction_count,
    avgMerge(avg_state) AS avg_amount
FROM transactions_daily
GROUP BY event_date
ORDER BY event_date;
