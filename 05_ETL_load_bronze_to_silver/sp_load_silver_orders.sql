
USE olist_ecommerce_dw;
GO

CREATE OR ALTER PROCEDURE etl.sp_load_silver_orders
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RunID BIGINT;
    DECLARE @RowCount BIGINT;

    INSERT INTO audit.ingestion_run (source_system, source_object, target_schema, target_table, status)
    VALUES ('bronze', 'olist_orders_dataset', 'silver', 'orders', 'STARTED');
    SET @RunID = SCOPE_IDENTITY();

    PRINT '>> Iniciando carga: silver.orders';

    BEGIN TRY
        TRUNCATE TABLE silver.orders;

        INSERT INTO silver.orders (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            created_at,
            source_system
        )
        SELECT 
            TRIM(order_id),  -- Usar columna correcta (no review_id)
            TRIM(customer_id),
            LOWER(TRIM(order_status)), -- Estandarizar status
            TRY_CONVERT(DATETIME2(3), order_purchase_timestamp), -- Casting seguro
            TRY_CONVERT(DATETIME2(3), order_approved_at),
            TRY_CONVERT(DATETIME2(3), order_delivered_carrier_date),
            TRY_CONVERT(DATETIME2(3), order_delivered_customer_date),
            TRY_CONVERT(DATETIME2(3), order_estimated_delivery_date),
            SYSDATETIME(),
            'bronze.olist_orders_dataset'
        FROM bronze.olist_orders
        WHERE order_id IS NOT NULL 
          AND TRIM(order_id) <> '';

        SET @RowCount = @@ROWCOUNT;

        UPDATE audit.ingestion_run
        SET load_ended_at = SYSDATETIME(), status = 'SUCCESS', rows_inserted = @RowCount
        WHERE run_id = @RunID;

        PRINT '   OK: ' + CAST(@RowCount AS VARCHAR) + ' filas cargadas (con cálculos automáticos).';
    END TRY
    BEGIN CATCH
        UPDATE audit.ingestion_run
        SET load_ended_at = SYSDATETIME(), status = 'FAILED', error_message = ERROR_MESSAGE()
        WHERE run_id = @RunID;
        PRINT '   ERROR: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO
