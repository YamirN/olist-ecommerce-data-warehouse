/*******************************************************************************
 * CAPA: GOLD (Modelo Dimensional / Star Schema)
 *
 * DESCRIPCIÓN:
 *   Creación de tablas de Dimensiones y Hechos optimizadas para BI.
 *   - Uso de Surrogate Keys (SK) tipo INT IDENTITY.
 *   - Desnormalización en dimensiones (Snowflake -> Star).
 *   - Indices Columnstore (opcional en SQL moderno) o B-Tree estándar.
 ******************************************************************************/

USE olist_ecommerce_dw;
GO

-- Crear esquema Gold si no existe
IF SCHEMA_ID('gold') IS NULL EXEC('CREATE SCHEMA gold');
GO

/* DROP HECHOS (hijos) */
IF OBJECT_ID('gold.fact_reviews', 'U') IS NOT NULL DROP TABLE gold.fact_reviews;
IF OBJECT_ID('gold.fact_order_items', 'U') IS NOT NULL DROP TABLE gold.fact_order_items;
IF OBJECT_ID('gold.fact_orders', 'U') IS NOT NULL DROP TABLE gold.fact_orders;
GO

/* DROP DIMENSIONES (padres) */
IF OBJECT_ID('gold.dim_seller', 'U') IS NOT NULL DROP TABLE gold.dim_seller;
IF OBJECT_ID('gold.dim_product', 'U') IS NOT NULL DROP TABLE gold.dim_product;
IF OBJECT_ID('gold.dim_customer', 'U') IS NOT NULL DROP TABLE gold.dim_customer;
IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL DROP TABLE gold.dim_date;
GO

-- ============================================================================
-- 1. DIMENSIONES (Descriptivas)
-- ============================================================================

-- 1.1 DIM_DATE (Calendario Maestro)
IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL DROP TABLE gold.dim_date;
CREATE TABLE gold.dim_date (
    date_key INT PRIMARY KEY,            
    date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name NVARCHAR(20),
    week_of_year INT,
    day_of_week INT,
    day_name NVARCHAR(20),
    is_weekend BIT,
    is_holiday BIT DEFAULT 0             
);
GO

-- 1.2 DIM_CUSTOMER
IF OBJECT_ID('gold.dim_customer', 'U') IS NOT NULL DROP TABLE gold.dim_customer;
CREATE TABLE gold.dim_customer (
    customer_sk INT IDENTITY(1,1) PRIMARY KEY,  
    customer_id NVARCHAR(100) NOT NULL,         
    customer_unique_id NVARCHAR(100),           
    customer_city NVARCHAR(100),
    customer_state NVARCHAR(10),
    effective_start_date DATETIME2(3) DEFAULT SYSDATETIME(), 
    effective_end_date DATETIME2(3),
    is_current BIT DEFAULT 1
);


-- 1.3 DIM_PRODUCT
-- Products + Category Translation
IF OBJECT_ID('gold.dim_product', 'U') IS NOT NULL DROP TABLE gold.dim_product;
CREATE TABLE gold.dim_product (
    product_sk INT IDENTITY(1,1) PRIMARY KEY,
    product_id NVARCHAR(100) NOT NULL,
    category_name NVARCHAR(100),           
    category_name_english NVARCHAR(100),   
    product_photos_qty INT,
    product_weight_g DECIMAL(10,2),
    product_length_cm DECIMAL(10,2),
    product_height_cm DECIMAL(10,2),
    product_width_cm DECIMAL(10,2),
    product_volume_cm3 DECIMAL(19,2)       -- Calculado
);


-- 1.4 DIM_SELLER
IF OBJECT_ID('gold.dim_seller', 'U') IS NOT NULL DROP TABLE gold.dim_seller;
CREATE TABLE gold.dim_seller (
    seller_sk INT IDENTITY(1,1) PRIMARY KEY,
    seller_id NVARCHAR(100) NOT NULL,
    seller_city NVARCHAR(100),
    seller_state NVARCHAR(10)
);

-- ============================================================================
-- 2. HECHOS (Métricas)
-- ============================================================================

-- 2.1 FACT_ORDERS (Cabecera)
-- Granularidad: Una fila por Orden
IF OBJECT_ID('gold.fact_orders', 'U') IS NOT NULL DROP TABLE gold.fact_orders;
CREATE TABLE gold.fact_orders (
    order_sk INT IDENTITY(1,1) PRIMARY KEY,
    order_id NVARCHAR(100) NOT NULL,       
    
    -- Foreign Keys a Dimensiones
    customer_sk INT NOT NULL FOREIGN KEY REFERENCES gold.dim_customer(customer_sk),
    purchase_date_key INT NOT NULL FOREIGN KEY REFERENCES gold.dim_date(date_key),
    delivered_date_key INT FOREIGN KEY REFERENCES gold.dim_date(date_key), -- Puede ser NULL
    estimated_date_key INT FOREIGN KEY REFERENCES gold.dim_date(date_key),
    
    -- Atributos de Estado
    order_status NVARCHAR(50),
    
    -- Métricas (Tiempo)
    lead_time_approved_days DECIMAL(10,2),   -- Compra -> Aprobación
    lead_time_shipping_days DECIMAL(10,2),   -- Aprobación -> Carrier
    lead_time_delivery_days DECIMAL(10,2),   -- Carrier -> Cliente
    total_delivery_days DECIMAL(10,2),       -- Compra -> Cliente
    delay_days DECIMAL(10,2),                -- Retraso vs Estimado (>0 es retraso)
    
    is_late_delivery BIT DEFAULT 0           -- Flag para análisis rápido
);


-- 2.2 FACT_ORDER_ITEMS (Detalle)
-- Granularidad: Una fila por Ítem en una Orden
-- Esta es la tabla principal de VENTAS
IF OBJECT_ID('gold.fact_order_items', 'U') IS NOT NULL DROP TABLE gold.fact_order_items;
CREATE TABLE gold.fact_order_items (
    order_item_sk INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Foreign Keys
    order_sk INT NOT NULL FOREIGN KEY REFERENCES gold.fact_orders(order_sk),
    product_sk INT NOT NULL FOREIGN KEY REFERENCES gold.dim_product(product_sk),
    seller_sk INT NOT NULL FOREIGN KEY REFERENCES gold.dim_seller(seller_sk),
    
    -- Métricas (Dinero)
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    total_item_value DECIMAL(10,2),  -- price + freight
    
    quantity INT DEFAULT 1           -- Olist separa items en filas, así que suele ser 1
);

-- 2.3 FACT_REVIEWS
-- Granularidad: Una fila por Review
IF OBJECT_ID('gold.fact_reviews', 'U') IS NOT NULL DROP TABLE gold.fact_reviews;
CREATE TABLE gold.fact_reviews (
    review_sk INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Foreign Keys
    order_sk INT NOT NULL FOREIGN KEY REFERENCES gold.fact_orders(order_sk),
    
    -- Atributos
    review_score TINYINT,      -- 1 a 5
    review_creation_date DATETIME2(3),
    review_answer_timestamp DATETIME2(3),
    
    -- Flags analíticos
    has_comment BIT,           -- ¿Dejó texto?
    is_positive BIT,           -- Score >= 4
    is_negative BIT            -- Score <= 2
);
GO

PRINT '=======================================================';
PRINT 'TABLAS GOLD (STAR SCHEMA) CREADAS EXITOSAMENTE';
PRINT '=======================================================';
