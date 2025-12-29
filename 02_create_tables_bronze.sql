USE olist_ecommerce_dw;
GO

-- ============================================================================
-- LIMPIEZA PREVIA: ELIMINAR TABLAS BRONZE (SI EXISTEN)
-- ============================================================================
-- Permite re-ejecutar el script sin errores de "tabla ya existe" y garantiza
-- que el esquema de columnas se mantenga sincronizado con los CSV originales.

DROP TABLE IF EXISTS bronze.olist_customers;
DROP TABLE IF EXISTS bronze.olist_geolocation;
DROP TABLE IF EXISTS bronze.olist_order_items;
DROP TABLE IF EXISTS bronze.olist_order_payments;
DROP TABLE IF EXISTS bronze.olist_order_reviews;
DROP TABLE IF EXISTS bronze.olist_orders;
DROP TABLE IF EXISTS bronze.olist_products;
DROP TABLE IF EXISTS bronze.olist_sellers;
DROP TABLE IF EXISTS bronze.product_category_name_translation;
DROP TABLE IF EXISTS audit.ingestion_run

-- 2. Recrear CORRECTAMENTE en esquema bronze
CREATE TABLE bronze.olist_customers (
	customer_id VARCHAR(MAX),
	customer_unique_id VARCHAR(MAX),
	customer_zip_code_prefix VARCHAR(MAX),
	customer_city VARCHAR(MAX),
	customer_state VARCHAR(MAX),

);

CREATE TABLE bronze.olist_geolocation (
	geolocation_zip_code_prefix VARCHAR(20),
	geolocation_lat VARCHAR(100),
	geolocation_lng VARCHAR(100),
	geolocation_city VARCHAR(50),
	geolocation_state VARCHAR(10),

);

CREATE TABLE bronze.olist_order_items (
	order_id VARCHAR(50),
	order_item_id VARCHAR(50),
	product_id VARCHAR(50),
	seller_id VARCHAR(50),
	shipping_limit_date VARCHAR(50),
	price VARCHAR(20),
	freight_value VARCHAR(20),

);

CREATE TABLE bronze.olist_order_payments (
	order_id VARCHAR(50),
	payment_sequential VARCHAR(5),
	payment_type VARCHAR(20),
	payment_installments VARCHAR(5),
	payment_value VARCHAR(20),

);

CREATE TABLE bronze.olist_order_reviews (
	review_id VARCHAR(MAX),
	order_id VARCHAR(MAX),
	review_score VARCHAR(MAX),
	review_comment_title NVARCHAR(MAX),
	review_comment_message VARCHAR(MAX),
	review_creation_date VARCHAR(MAX),
	review_answer_timestamp VARCHAR(MAX),

);

CREATE TABLE bronze.olist_orders (
	order_id VARCHAR(50),
	customer_id VARCHAR(50),
	order_status VARCHAR(20),
	order_purchase_timestamp VARCHAR(50),
	order_approved_at VARCHAR(50),
	order_delivered_carrier_date VARCHAR(50),
	order_delivered_customer_date VARCHAR(50),
	order_estimated_delivery_date VARCHAR(50),

);

CREATE TABLE bronze.olist_products (
	product_id VARCHAR(50),
	product_category_name VARCHAR(50),
	product_name_lenght VARCHAR(5),
	product_description_lenght VARCHAR(10),
	product_photos_qty VARCHAR(10),
	product_weight_g VARCHAR(20),
	product_length_cm VARCHAR(20),
	product_height_cm VARCHAR(20),
	product_width_cm VARCHAR(20),

);

CREATE TABLE bronze.olist_sellers (
	seller_id VARCHAR(50),
	seller_zip_code_prefix VARCHAR(10),
	seller_city VARCHAR(50),
	seller_state VARCHAR(5),

);

CREATE TABLE bronze.product_category_name_translation (
	product_category_name VARCHAR(200),
	product_category_name_english VARCHAR(200),

);

CREATE TABLE audit.ingestion_run (
    run_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_system NVARCHAR(100),
    source_object NVARCHAR(200),
    target_schema NVARCHAR(50),
    target_table NVARCHAR(200),
    source_path NVARCHAR(500),
    load_started_at DATETIME2(3) DEFAULT SYSDATETIME(),
    load_ended_at DATETIME2(3),
    status NVARCHAR(20),
    rows_inserted BIGINT,
    rows_updated BIGINT,
    rows_deleted BIGINT,
    error_message NVARCHAR(MAX)
);
GO
