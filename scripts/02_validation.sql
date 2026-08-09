-- Core row and key checks
SELECT 'customers' dataset, count(*) rows, count(DISTINCT customer_id) distinct_keys FROM dea_retail_curateddatabase.customers
UNION ALL SELECT 'products', count(*), count(DISTINCT product_id) FROM dea_retail_curateddatabase.products
UNION ALL SELECT 'orders', count(*), count(DISTINCT order_id) FROM dea_retail_curateddatabase.orders;

SELECT order_id, count(*) duplicates
FROM dea_retail_curateddatabase.orders GROUP BY order_id HAVING count(*) > 1;

SELECT o.order_id FROM dea_retail_curateddatabase.orders o
LEFT JOIN dea_retail_curateddatabase.customers c ON o.customer_id=c.customer_id
WHERE c.customer_id IS NULL;

SELECT i.order_id, i.line_number FROM dea_retail_curateddatabase.order_items i
LEFT JOIN dea_retail_curateddatabase.orders o ON i.order_id=o.order_id
LEFT JOIN dea_retail_curateddatabase.products p ON i.product_id=p.product_id
WHERE o.order_id IS NULL OR p.product_id IS NULL;

SELECT * FROM dea_retail_curateddatabase.orders
WHERE total_amount < 0 OR status NOT IN ('PLACED','PAID','SHIPPED','DELIVERED','CANCELLED');

-- Good-batch expected results: five orders, seven items, total non-cancelled revenue 595.00.
SELECT count(*) orders, cast(sum(total_amount) AS decimal(18,2)) all_order_total,
       cast(sum(CASE WHEN status <> 'CANCELLED' THEN total_amount ELSE 0 END) AS decimal(18,2)) non_cancelled_revenue
FROM dea_retail_curateddatabase.orders;

SELECT * FROM dea_retail_gold.daily_sales ORDER BY order_date;
SELECT * FROM dea_retail_gold.customer_360 ORDER BY customer_id;

-- Idempotency: rerun the Glue job, rerun this query, and confirm rows = distinct keys.
SELECT count(*) rows, count(DISTINCT order_id) distinct_orders FROM dea_retail_curateddatabase.orders;
