-- =============================================
-- ecommerce-nifi-pipeline
-- ClickHouse Datamart Views (OLAP Analytical Layer)
-- =============================================

USE ecommerce;

-- 1. Kinerja Penjualan Harian (Revenue, Freight, Order Count)
CREATE VIEW IF NOT EXISTS ecommerce.view_daily_sales_performance AS
SELECT
    toDate(o.order_purchase_timestamp) AS order_date,
    count(DISTINCT o.order_id) AS total_orders,
    sum(oi.price) AS total_product_revenue,
    sum(oi.freight_value) AS total_freight_cost,
    sum(oi.price + oi.freight_value) AS total_gross_revenue,
    avg(oi.price) AS avg_order_value
FROM ecommerce.orders AS o
INNER JOIN ecommerce.order_items AS oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
GROUP BY order_date
ORDER BY order_date DESC;

-- 2. Top Kategori Produk Berdasarkan Revenue
CREATE VIEW IF NOT EXISTS ecommerce.view_top_product_categories AS
SELECT
    coalesce(t.product_category_name_english, p.product_category_name, 'unknown') AS category_name,
    count(DISTINCT oi.order_id) AS total_orders_sold,
    sum(oi.price) AS total_revenue,
    avg(oi.price) AS avg_item_price
FROM ecommerce.order_items AS oi
INNER JOIN ecommerce.orders AS o ON oi.order_id = o.order_id
LEFT JOIN ecommerce.products AS p ON oi.product_id = p.product_id
LEFT JOIN ecommerce.product_category_name_translation AS t ON p.product_category_name = t.product_category_name
WHERE o.order_status = 'delivered'
GROUP BY category_name
ORDER BY total_revenue DESC;

-- 3. Performa Penjualan Berdasarkan Wilayah (State Customer)
CREATE VIEW IF NOT EXISTS ecommerce.view_sales_by_customer_state AS
SELECT
    c.customer_state AS state,
    count(DISTINCT c.customer_unique_id) AS total_unique_customers,
    count(DISTINCT o.order_id) AS total_orders,
    sum(oi.price) AS total_revenue
FROM ecommerce.orders AS o
INNER JOIN ecommerce.order_items AS oi ON o.order_id = oi.order_id
INNER JOIN ecommerce.customers AS c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
GROUP BY state
ORDER BY total_revenue DESC;
