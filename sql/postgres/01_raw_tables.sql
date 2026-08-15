-- public.customers definition
-- Drop table
-- DROP TABLE public.customers;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

create table public.customers ( customer_id text not null,
customer_unique_id text null,
customer_zip_code_prefix text null,
customer_city text null,
customer_state text null,
updated_at timestamp(0) default now() null,
constraint customers_pkey primary key (customer_id));
-- Table Triggers

create trigger trg_customers_updated_at before
update
    on
    public.customers for each row execute function set_updated_at();
-- public.geolocation definition
-- Drop table
-- DROP TABLE public.geolocation;

create table public.geolocation ( geolocation_zip_code_prefix text null,
geolocation_lat float8 null,
geolocation_lng float8 null,
geolocation_city text null,
geolocation_state text null,
updated_at timestamp(0) default now() null);
-- Table Triggers

create trigger trg_geolocation_updated_at before
update
    on
    public.geolocation for each row execute function set_updated_at();
-- public.order_items definition
-- Drop table
-- DROP TABLE public.order_items;

create table public.order_items ( order_id text not null,
order_item_id int4 not null,
product_id text null,
seller_id text null,
shipping_limit_date timestamp null,
price numeric(10, 2) null,
freight_value numeric(10, 2) null,
updated_at timestamp(0) default now() null,
constraint order_items_pkey primary key (order_id,
order_item_id));
-- Table Triggers

create trigger trg_order_items_updated_at before
update
    on
    public.order_items for each row execute function set_updated_at();
-- public.order_payments definition
-- Drop table
-- DROP TABLE public.order_payments;

create table public.order_payments ( order_id text not null,
payment_sequential int4 not null,
payment_type text null,
payment_installments int4 null,
payment_value numeric(10, 2) null,
updated_at timestamp(0) default now() null,
constraint order_payments_pkey primary key (order_id,
payment_sequential));
-- Table Triggers

create trigger trg_order_payments_updated_at before
update
    on
    public.order_payments for each row execute function set_updated_at();
-- public.order_reviews definition
-- Drop table
-- DROP TABLE public.order_reviews;

create table public.order_reviews ( review_id text null,
order_id text null,
review_score int2 null,
review_comment_title text null,
review_comment_message text null,
review_creation_date timestamp null,
review_answer_timestamp timestamp null,
updated_at timestamp(0) default now() null);
-- Table Triggers

create trigger trg_order_reviews_updated_at before
update
    on
    public.order_reviews for each row execute function set_updated_at();
-- public.orders definition
-- Drop table
-- DROP TABLE public.orders;

create table public.orders ( order_id text not null,
customer_id text null,
order_status text null,
order_purchase_timestamp timestamp null,
order_approved_at timestamp null,
order_delivered_carrier_date timestamp null,
order_delivered_customer_date timestamp null,
order_estimated_delivery_date timestamp null,
updated_at timestamp(0) default now() null,
constraint orders_pkey primary key (order_id));
-- Table Triggers

create trigger trg_orders_updated_at before
update
    on
    public.orders for each row execute function set_updated_at();
-- public.product_category_name_translation definition
-- Drop table
-- DROP TABLE public.product_category_name_translation;

create table public.product_category_name_translation ( product_category_name text not null,
product_category_name_english text null,
updated_at timestamp(0) default now() null,
constraint product_category_name_translation_pkey primary key (product_category_name));
-- Table Triggers

create trigger trg_product_category_name_translation_updated_at before
update
    on
    public.product_category_name_translation for each row execute function set_updated_at();
-- public.products definition
-- Drop table
-- DROP TABLE public.products;

create table public.products ( product_id text not null,
product_category_name text null,
product_name_lenght int4 null,
product_description_lenght int4 null,
product_photos_qty int4 null,
product_weight_g int4 null,
product_length_cm int4 null,
product_height_cm int4 null,
product_width_cm int4 null,
updated_at timestamp(0) default now() null,
constraint products_pkey primary key (product_id));
-- Table Triggers

create trigger trg_products_updated_at before
update
    on
    public.products for each row execute function set_updated_at();
-- public.sellers definition
-- Drop table
-- DROP TABLE public.sellers;

create table public.sellers ( seller_id text not null,
seller_zip_code_prefix text null,
seller_city text null,
seller_state text null,
updated_at timestamp(0) default now() null,
constraint sellers_pkey primary key (seller_id));
-- Table Triggers

create trigger trg_sellers_updated_at before
update
    on
    public.sellers for each row execute function set_updated_at();
