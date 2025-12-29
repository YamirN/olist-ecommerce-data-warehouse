USE olist_ecommerce_dw;
GO

CREATE OR ALTER PROCEDURE etl.sp_load_silver_order_payments
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunID BIGINT;
    DECLARE @RowCount BIGINT;

    INSERT INTO audit.ingestion_run (source_system, source_object, target_schema, target_table, status)
    VALUES ('bronze', 'olist_order_payments', 'silver', 'order_payments', 'STARTED');
    SET @RunID = SCOPE_IDENTITY();

    PRINT '>> Iniciando carga: silver.order_payments';

    BEGIN TRY
        TRUNCATE TABLE silver.order_payments;

        INSERT INTO silver.order_payments (
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value,
            created_at,
            source_system
        )
        SELECT
            TRIM(order_id),
            TRY_CAST(payment_sequential AS INT),
            LOWER(TRIM(payment_type)),
            TRY_CAST(payment_installments AS INT),
            TRY_CAST(REPLACE(payment_value, ',', '.') AS DECIMAL(10,2)),
            SYSDATETIME(),
            'bronze.olist_order_payments'
        FROM bronze.olist_order_payments
        WHERE order_id IS NOT NULL
          AND TRIM(order_id) <> ''
          AND payment_type IS NOT NULL;

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
