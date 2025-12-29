USE olist_ecommerce_dw;
GO

CREATE OR ALTER PROCEDURE etl.sp_load_silver_sellers
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunID BIGINT;
    DECLARE @RowCount BIGINT;

    INSERT INTO audit.ingestion_run (source_system, source_object, target_schema, target_table, status)
    VALUES ('bronze', 'olist_sellers', 'silver', 'sellers', 'STARTED');
    SET @RunID = SCOPE_IDENTITY();

    PRINT '>> Iniciando carga: silver.sellers';

    BEGIN TRY
        TRUNCATE TABLE silver.sellers;

        INSERT INTO silver.sellers (
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state,
            created_at,
            source_system
        )
        SELECT
            TRIM(seller_id),
            LEFT(TRIM(seller_zip_code_prefix), 10),
            TRIM(seller_city),
            UPPER(LEFT(TRIM(seller_state), 2)),
            SYSDATETIME(),
            'bronze.olist_sellers'
        FROM bronze.olist_sellers
        WHERE seller_id IS NOT NULL
          AND TRIM(seller_id) <> '';

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
