-- Run after curated tables have been crawled into dea_retail_curated.
-- Replace <BUCKET> before running.

DROP TABLE IF EXISTS dea_retail_gold.daily_sales;

CREATE TABLE dea_retail_gold.daily_sales
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://<BUCKET>/gold/daily_sales/'
) AS
SELECT
  o.order_date,
  count(DISTINCT o.order_id) AS order_count,
  cast(sum(o.total_amount) AS decimal(18,2)) AS revenue,
  cast(avg(o.total_amount) AS decimal(18,2)) AS average_order_value,
  current_timestamp AS refreshed_at
FROM dea_retail_curated.orders o
WHERE o.status <> 'CANCELLED'
GROUP BY o.order_date;

DROP TABLE IF EXISTS dea_retail_gold.customer_360;

CREATE TABLE dea_retail_gold.customer_360
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://<BUCKET>/gold/customer_360/'
) AS
SELECT
  c.customer_id,
  c.country,
  count(DISTINCT o.order_id) AS non_cancelled_orders,
  cast(coalesce(sum(o.total_amount), 0) AS decimal(18,2)) AS lifetime_value,
  max(o.order_date) AS latest_order_date,
  current_timestamp AS refreshed_at
FROM dea_retail_curated.customers c
LEFT JOIN dea_retail_curated.orders o
  ON c.customer_id = o.customer_id
 AND o.status <> 'CANCELLED'
GROUP BY c.customer_id, c.country;
