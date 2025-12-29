USE olist_ecommerce_dw;
GO

CREATE OR ALTER PROCEDURE etl.sp_load_silver_order_items
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunID BIGINT;
    DECLARE @RowCount BIGINT;

    INSERT INTO audit.ingestion_run (source_system, source_object, target_schema, target_table, status)
    VALUES ('bronze', 'olist_order_items', 'silver', 'order_items', 'STARTED');
    SET @RunID = SCOPE_IDENTITY();

    PRINT '>> Iniciando carga: silver.order_items';

    BEGIN TRY
        TRUNCATE TABLE silver.order_items;

        INSERT INTO silver.order_items (
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value,
            created_at,
            source_system
        )
        SELECT
            TRIM(order_id),
            TRY_CAST(order_item_id AS INT),
            TRIM(product_id),
            TRIM(seller_id),
            TRY_CONVERT(DATETIME2(3), shipping_limit_date),
            TRY_CAST(REPLACE(price, ',', '.') AS DECIMAL(10,2)),
            TRY_CAST(REPLACE(freight_value, ',', '.') AS DECIMAL(10,2)),
            SYSDATETIME(),
            'bronze.olist_order_items'
        FROM bronze.olist_order_items
        WHERE order_id IS NOT NULL
          AND TRIM(order_id) <> ''
          AND TRY_CAST(order_item_id AS INT) IS NOT NULL
          AND product_id IS NOT NULL
          AND seller_id IS NOT NULL;

        SET @RowCount = @@ROWCOUNT;

        UPDATE audit.ingestion_run
        SET load_ended_at = SYSDATETIME(),
            status = 'SUCCESS',
            rows_inserted = @RowCount
        WHERE run_id = @RunID;

        PRINT '   OK: ' + CAST(@RowCount AS VARCHAR) + ' filas cargadas.';
    END TRY
    BEGIN CATCH
        UPDATE audit.ingestion_run
        SET load_ended_at = SYSDATETIME(),
            status = 'FAILED',
            error_message = ERROR_MESSAGE()
        WHERE run_id = @RunID;

        PRINT '   ERROR: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO
