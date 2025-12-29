/*******************************************************************************
 * PROCESO: ETL Silver -> Gold (Dimensional Load)
 ******************************************************************************/

USE olist_ecommerce_dw;
GO

-- ============================================================================
-- 1. SP: CARGAR DIM_DATE (Generación Automática)
-- ============================================================================
CREATE OR ALTER PROCEDURE etl.sp_load_gold_dim_date
AS
BEGIN
    SET NOCOUNT ON;
    PRINT '>> Verificando Gold.Dim_Date...';
    
    -- Si la tabla ya tiene datos, salir
    IF EXISTS (SELECT 1 FROM gold.dim_date WHERE date_key <> 19000101)
    BEGIN
        PRINT '   La tabla ya tiene datos. Saltando carga.';
        RETURN;
    END

    PRINT '   Generando calendario completo (2016-2022)...';
    
    -- Limpiar
    DELETE FROM gold.dim_date;
    
    -- Generar todas las fechas en un solo INSERT (incluye fila N/A)
    ;WITH cte_dates AS (
        -- Fila "Desconocido"
        SELECT 
            CAST('1900-01-01' AS DATE) AS date_value,
            0 AS row_num
        
        UNION ALL
        
        -- Rango real de fechas
        SELECT 
            CAST('2016-01-01' AS DATE),
            1
        
        UNION ALL
        
        SELECT 
            DATEADD(DAY, 1, date_value),
            row_num + 1
        FROM cte_dates
        WHERE date_value < '2022-12-31'
    )
    INSERT INTO gold.dim_date (
        date_key, 
        date, 
        year, 
        quarter, 
        month, 
        month_name, 
        week_of_year, 
        day_of_week, 
        day_name, 
        is_weekend
    )
    SELECT 
        CASE 
            WHEN row_num = 0 THEN 19000101 
            ELSE YEAR(date_value) * 10000 + MONTH(date_value) * 100 + DAY(date_value)
        END AS date_key,
        date_value AS date,
        CASE WHEN row_num = 0 THEN 1900 ELSE YEAR(date_value) END AS year,
        CASE WHEN row_num = 0 THEN 1 ELSE DATEPART(QUARTER, date_value) END AS quarter,
        CASE WHEN row_num = 0 THEN 1 ELSE MONTH(date_value) END AS month,
        CASE WHEN row_num = 0 THEN 'N/A' ELSE DATENAME(MONTH, date_value) END AS month_name,
        CASE WHEN row_num = 0 THEN 1 ELSE DATEPART(WEEK, date_value) END AS week_of_year,
        CASE WHEN row_num = 0 THEN 1 ELSE DATEPART(WEEKDAY, date_value) END AS day_of_week,
        CASE WHEN row_num = 0 THEN 'N/A' ELSE DATENAME(WEEKDAY, date_value) END AS day_name,
        CASE 
            WHEN row_num = 0 THEN 0 
            WHEN DATEPART(WEEKDAY, date_value) IN (1, 7) THEN 1 
            ELSE 0 
        END AS is_weekend
    FROM cte_dates
    OPTION (MAXRECURSION 0);  -- Sin límite de recursión
    
    PRINT '   OK: ' + CAST(@@ROWCOUNT AS VARCHAR(20)) + ' días generados.';
END;
GO



-- ============================================================================
-- 2. SP: CARGAR DIM_CUSTOMER
-- ============================================================================
CREATE OR ALTER PROCEDURE etl.sp_load_gold_dim_customer
AS
BEGIN
    SET NOCOUNT ON;
    PRINT '>> Cargando Gold.Dim_Customer...';

    DELETE FROM gold.dim_customer;

    INSERT INTO gold.dim_customer (
        customer_id, 
		customer_unique_id, 
		customer_city, 
		customer_state
    )
    SELECT DISTINCT
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state

    FROM silver.customers c
    LEFT JOIN silver.geolocation g 
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
    AND c.customer_city = g.geolocation_city;

    PRINT '   OK: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' clientes cargados.';
END;
GO

-- ============================================================================
-- 3. SP: CARGAR DIM_PRODUCT
-- ============================================================================
CREATE OR ALTER PROCEDURE etl.sp_load_gold_dim_product
AS
BEGIN
    SET NOCOUNT ON;
    PRINT '>> Cargando Gold.Dim_Product...';

    DELETE FROM gold.dim_product;

    INSERT INTO gold.dim_product (
        product_id, 
		category_name, 
		category_name_english,
        product_photos_qty, 
		product_weight_g, 
        product_length_cm, 
		product_height_cm, 
		product_width_cm, 
		product_volume_cm3
    )
    SELECT 
        product_id,
        product_category_name,
        product_category_name_english,
        product_photos_qty,
        product_weight_g,
        product_length_cm, 
        product_height_cm, 
        product_width_cm,
        product_volume_cm3

    FROM silver.products;

    PRINT '   OK: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' productos cargados.';
END;
GO

-- ============================================================================
-- 4. SP: CARGAR DIM_SELLER
-- ============================================================================
CREATE OR ALTER PROCEDURE etl.sp_load_gold_dim_seller
AS
BEGIN
    SET NOCOUNT ON;
    PRINT '>> Cargando Gold.Dim_Seller...';

    DELETE FROM gold.dim_seller;

    INSERT INTO gold.dim_seller (
		seller_id, 
		seller_city, 
		seller_state
	)
    SELECT 
		seller_id, 
		seller_city, 
		seller_state
    FROM silver.sellers;

    PRINT '   OK: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' vendedores cargados.';
END;
GO

-- ============================================================================
-- 5. SP: CARGAR FACT_ORDERS
-- ============================================================================
CREATE OR ALTER PROCEDURE etl.sp_load_gold_fact_orders
AS
BEGIN
    SET NOCOUNT ON;
    PRINT '>> Cargando Gold.Fact_Orders ...';

    -- Borramos datos previos (usar DELETE para evitar bloqueos raros de FKs)
    DELETE FROM gold.fact_orders;
    DBCC CHECKIDENT ('gold.fact_orders', RESEED, 0);

    INSERT INTO gold.fact_orders (
        order_id, customer_sk, 
		purchase_date_key, 
        delivered_date_key, 
		estimated_date_key, 
        order_status, 
        lead_time_approved_days, 
		lead_time_shipping_days, 
		lead_time_delivery_days,
        total_delivery_days, 
		delay_days, 
		is_late_delivery
    )
    SELECT 
        o.order_id,
        dc.customer_sk,
        -- Fecha de compra SIEMPRE existe (NOT NULL)
        COALESCE(FORMAT(o.order_purchase_timestamp, 'yyyyMMdd'), 19000101),
        
        -- Fechas opcionales: SI es NULL, dejamos NULL (Quitamos el COALESCE forzado)
        CASE WHEN o.order_delivered_customer_date IS NULL THEN NULL 
             ELSE FORMAT(o.order_delivered_customer_date, 'yyyyMMdd') END,
             
        CASE WHEN o.order_estimated_delivery_date IS NULL THEN NULL 
             ELSE FORMAT(o.order_estimated_delivery_date, 'yyyyMMdd') END,
             
        o.order_status,
        -- Métricas
        DATEDIFF(HOUR, o.order_purchase_timestamp, o.order_approved_at) / 24.0,
        DATEDIFF(HOUR, o.order_approved_at, o.order_delivered_carrier_date) / 24.0,
        DATEDIFF(HOUR, o.order_delivered_carrier_date, o.order_delivered_customer_date) / 24.0,
        o.delivery_days,
        o.delay_days,
        CASE WHEN o.delay_days > 0 THEN 1 ELSE 0 END
    FROM silver.orders o
    INNER JOIN gold.dim_customer dc ON o.customer_id = dc.customer_id;

    PRINT '   OK: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' órdenes cargadas (con NULLs permitidos).';
END;
GO


-- ============================================================================
-- 6. SP: CARGAR FACT_ORDER_ITEMS
-- ============================================================================
CREATE OR ALTER PROCEDURE etl.sp_load_gold_fact_order_items
AS
BEGIN
    SET NOCOUNT ON;
    PRINT '>> Cargando Gold.Fact_Order_Items...';

    DELETE FROM gold.fact_order_items;

    INSERT INTO gold.fact_order_items (
        order_sk, 
		product_sk, 
		seller_sk,
        price, 
		freight_value, 
		total_item_value, 
		quantity
    )
    SELECT 
        fo.order_sk,
        dp.product_sk,
        ds.seller_sk,
        oi.price,
        oi.freight_value,
        oi.total_item_value,
        1 -- Quantity fija (Olist desglosa items)
    FROM silver.order_items oi
    INNER JOIN gold.fact_orders fo ON oi.order_id = fo.order_id
    INNER JOIN gold.dim_product dp ON oi.product_id = dp.product_id
    INNER JOIN gold.dim_seller ds ON oi.seller_id = ds.seller_id;

    PRINT '   OK: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' items cargados.';
END;
GO

-- ============================================================================
-- 7. SP: CARGAR FACT_ORDER_REVIEWS
-- ============================================================================

CREATE OR ALTER PROCEDURE etl.sp_load_gold_fact_reviews
AS
BEGIN
    SET NOCOUNT ON;
    PRINT '>> Cargando Gold.Fact_Reviews...';

    -- Limpiar tabla
    DELETE FROM gold.fact_reviews;
    DBCC CHECKIDENT ('gold.fact_reviews', RESEED, 0);

    INSERT INTO gold.fact_reviews (
        order_sk,
        review_score,
        review_creation_date,
        review_answer_timestamp,
        has_comment,
        is_positive,
        is_negative
    )
    SELECT 
        fo.order_sk,
        r.review_score,
        r.review_creation_date,
        r.review_answer_timestamp,
        -- Flags analíticos
        CASE 
            WHEN r.review_comment_message IS NOT NULL 
             AND LEN(TRIM(r.review_comment_message)) > 0 
            THEN 1 
            ELSE 0 
        END AS has_comment,
        CASE WHEN r.review_score >= 4 THEN 1 ELSE 0 END AS is_positive,
        CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END AS is_negative
    FROM silver.order_reviews r
    INNER JOIN gold.fact_orders fo ON r.order_id = fo.order_id;

    PRINT '   OK: ' + CAST(@@ROWCOUNT AS VARCHAR(20)) + ' reviews cargadas.';
END;
GO

-- ============================================================================
-- 8. ORQUESTADOR GOLD
-- ============================================================================
CREATE OR ALTER PROCEDURE etl.sp_gold_orchestrator
AS
BEGIN
    SET XACT_ABORT ON;
    PRINT '=== INICIANDO CARGA GOLD (STAR SCHEMA) ===';
    
    BEGIN TRY
		-- 1. LIMPIEZA PREVIA (Orden Inverso para respetar FKs)
        -- Borramos Facts primero para liberar a las Dimensiones
		PRINT '>> Limpiando tablas de Hechos...';
        DELETE FROM gold.fact_order_items;
        DELETE FROM gold.fact_orders;
        -- 1. Dimensiones
        EXEC etl.sp_load_gold_dim_date;
        EXEC etl.sp_load_gold_dim_customer;
        EXEC etl.sp_load_gold_dim_product;
        EXEC etl.sp_load_gold_dim_seller;

        -- 2. Hechos (Dependen de Dimensiones)
        EXEC etl.sp_load_gold_fact_orders;
        EXEC etl.sp_load_gold_fact_order_items;
        EXEC etl.sp_load_gold_fact_reviews;

        PRINT '=== CARGA GOLD COMPLETADA ===';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR EN CARGA GOLD: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

EXEC etl.sp_gold_orchestrator



