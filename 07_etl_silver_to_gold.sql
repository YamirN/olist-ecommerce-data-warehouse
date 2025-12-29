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
    
    -- Si la tabla ya tiene datos normales (ej. > 100 filas), asumimos que está cargada
    IF (SELECT COUNT(*) FROM gold.dim_date) > 100
    BEGIN
        PRINT '   La tabla ya tiene datos. Saltando carga.';
        RETURN;
    END

    -- Si está vacía o casi vacía, limpiamos y recargamos
    DELETE FROM gold.dim_date; 
    
    PRINT '   Generando fila N/A (19000101)...';
    -- 1. Insertar el "Miembro Desconocido" para manejar NULLs
    INSERT INTO gold.dim_date (
        date_key, date, year, quarter, month, month_name, 
        week_of_year, day_of_week, day_name, is_weekend
    )
    VALUES (
        19000101, '1900-01-01', 1900, 1, 1, 'N/A', 1, 1, 'N/A', 0
    );

    -- 2. Generar el calendario normal
    PRINT '   Generando calendario 2016-2022...';
    DECLARE @StartDate DATE = '2016-01-01';
    DECLARE @EndDate DATE = '2022-12-31';

    WHILE @StartDate <= @EndDate
    BEGIN
        INSERT INTO gold.dim_date (
            date_key, date, year, quarter, month, month_name, 
            week_of_year, day_of_week, day_name, is_weekend
        )
        SELECT 
            YEAR(@StartDate) * 10000 + MONTH(@StartDate) * 100 + DAY(@StartDate),
            @StartDate,
            YEAR(@StartDate),
            DATEPART(QUARTER, @StartDate),
            MONTH(@StartDate),
            DATENAME(MONTH, @StartDate),
            DATEPART(WEEK, @StartDate),
            DATEPART(WEEKDAY, @StartDate),
            DATENAME(WEEKDAY, @StartDate),
            CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (1, 7) THEN 1 ELSE 0 END
        
        SET @StartDate = DATEADD(DAY, 1, @StartDate);
    END
    PRINT '   OK: Calendario generado con éxito.';
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

    -- En DWH real usaríamos MERGE para SCD Tipo 2, aquí hacemos Full Refresh simple
    DELETE FROM gold.dim_customer;

    INSERT INTO gold.dim_customer (
        customer_id, customer_unique_id, customer_city, customer_state,
        geolocation_lat, geolocation_lng
    )
    SELECT DISTINCT
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        g.geolocation_lat,
        g.geolocation_lng
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
        product_id, category_name, category_name_english,
        product_photos_qty, product_weight_g, 
        product_length_cm, product_height_cm, product_width_cm, product_volume_cm3
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

    INSERT INTO gold.dim_seller (seller_id, seller_city, seller_state)
    SELECT seller_id, seller_city, seller_state
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
    PRINT '>> Cargando Gold.Fact_Orders (Versión con NULLs)...';

    -- Borramos datos previos (usar DELETE para evitar bloqueos raros de FKs)
    DELETE FROM gold.fact_orders;
    DBCC CHECKIDENT ('gold.fact_orders', RESEED, 0);

    INSERT INTO gold.fact_orders (
        order_id, customer_sk, purchase_date_key, 
        delivered_date_key, estimated_date_key, -- Estas ahora pueden ser NULL
        order_status, 
        lead_time_approved_days, lead_time_shipping_days, lead_time_delivery_days,
        total_delivery_days, delay_days, is_late_delivery
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
        order_sk, product_sk, seller_sk,
        price, freight_value, total_item_value, quantity
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
-- 7. ORQUESTADOR GOLD
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
        -- EXEC etl.sp_load_gold_fact_reviews; (Tarea para ti)

        PRINT '=== CARGA GOLD COMPLETADA ===';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR EN CARGA GOLD: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

EXEC etl.sp_gold_orchestrator


