-- =============================================
-- ecommerce-nifi-pipeline
-- ClickHouse landing tables (OLAP) — Olist dataset
-- Struktur disamakan dengan PostgreSQL source (tanpa prefix raw_)
-- Engine: MergeTree
-- =============================================

CREATE DATABASE IF NOT EXISTS ecommerce;

-- =============================================
-- customers
-- =============================================
CREATE TABLE IF NOT EXISTS ecommerce.customers (
    customer_id              String,
    customer_unique_id       Nullable(String),
    customer_zip_code_prefix Nullable(String),
    customer_city            Nullable(String),
    customer_state           Nullable(String),
    updated_at               DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (customer_id);

-- =============================================
-- geolocation
-- =============================================
CREATE TABLE IF NOT EXISTS ecommerce.geolocation (
    geolocation_zip_code_prefix String,
    geolocation_lat             Nullable(Float64),
    geolocation_lng             Nullable(Float64),
    geolocation_city            Nullable(String),
    geolocation_state           Nullable(String),
    updated_at                  DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (geolocation_zip_code_prefix);

-- =============================================
-- order_items
-- =============================================
CREATE TABLE IF NOT EXISTS ecommerce.order_items (
    order_id            String,
    order_item_id       UInt32,
    product_id          Nullable(String),
    seller_id           Nullable(String),
    shipping_limit_date Nullable(DateTime),
    price               Nullable(Decimal(10,2)),
    freight_value       Nullable(Decimal(10,2)),
    updated_at          DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (order_id, order_item_id);

-- =============================================
-- order_payments
-- =============================================
CREATE TABLE IF NOT EXISTS ecommerce.order_payments (
    order_id             String,
    payment_sequential   UInt32,
    payment_type         Nullable(String),
    payment_installments Nullable(Int32),
    payment_value        Nullable(Decimal(10,2)),
    updated_at           DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (order_id, payment_sequential);

-- =============================================
-- order_reviews
-- =============================================
CREATE TABLE IF NOT EXISTS ecommerce.order_reviews (
    review_id               String,
    order_id                String,
    review_score            Nullable(Int16),
    review_comment_title    Nullable(String),
    review_comment_message  Nullable(String),
    review_creation_date    Nullable(DateTime),
    review_answer_timestamp Nullable(DateTime),
    updated_at              DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (order_id, review_id);

-- =============================================
-- orders
-- =============================================
CREATE TABLE IF NOT EXISTS ecommerce.orders (
    order_id                     String,
    customer_id                  Nullable(String),
    order_status                 Nullable(String),
    order_purchase_timestamp     Nullable(DateTime),
    order_approved_at            Nullable(DateTime),
    order_delivered_carrier_date Nullable(DateTime),
    order_delivered_customer_date Nullable(DateTime),
    order_estimated_delivery_date Nullable(DateTime),
    updated_at                   DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (order_id);

-- =============================================
-- product_category_name_translation
-- =============================================
CREATE TABLE IF NOT EXISTS ecommerce.product_category_name_translation (
    product_category_name         String,
    product_category_name_english Nullable(String),
    updated_at                    DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (product_category_name);

-- =============================================
-- products
-- =============================================
CREATE TABLE IF NOT EXISTS ecommerce.products (
    product_id              String,
    product_category_name   Nullable(String),
    product_name_lenght     Nullable(Int32),
    product_description_lenght Nullable(Int32),
    product_photos_qty      Nullable(Int32),
    product_weight_g        Nullable(Int32),
    product_length_cm       Nullable(Int32),
    product_height_cm       Nullable(Int32),
    product_width_cm        Nullable(Int32),
    updated_at              DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (product_id);

-- =============================================
-- sellers
-- =============================================
CREATE TABLE IF NOT EXISTS ecommerce.sellers (
    seller_id              String,
    seller_zip_code_prefix Nullable(String),
    seller_city            Nullable(String),
    seller_state           Nullable(String),
    updated_at             DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (seller_id);