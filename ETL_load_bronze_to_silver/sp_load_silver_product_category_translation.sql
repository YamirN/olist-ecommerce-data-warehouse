USE olist_ecommerce_dw;
GO

CREATE OR ALTER PROCEDURE etl.sp_load_silver_product_category_translation
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunID BIGINT;
    DECLARE @RowCount BIGINT;

    INSERT INTO audit.ingestion_run (source_system, source_object, target_schema, target_table, status)
    VALUES ('bronze', 'product_category_name_translation', 'silver', 'product_category_translation', 'STARTED');
    SET @RunID = SCOPE_IDENTITY();

    PRINT '>> Iniciando carga: silver.product_category_translation';

    BEGIN TRY
        TRUNCATE TABLE silver.product_category_translation;

        INSERT INTO silver.product_category_translation (
            product_category_name,
            product_category_name_english,
            created_at,
            source_system
        )
        SELECT
            TRIM(product_category_name),
            TRIM(product_category_name_english),
            SYSDATETIME(),
            'bronze.product_category_name_translation'
        FROM bronze.product_category_name_translation
        WHERE product_category_name IS NOT NULL
          AND TRIM(product_category_name) <> '';

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
