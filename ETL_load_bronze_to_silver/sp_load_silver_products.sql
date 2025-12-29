USE olist_ecommerce_dw;
GO

CREATE OR ALTER PROCEDURE etl.sp_load_silver_products
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunID BIGINT;
    DECLARE @RowCount BIGINT;

    INSERT INTO audit.ingestion_run (source_system, source_object, target_schema, target_table, status)
    VALUES ('bronze', 'olist_products', 'silver', 'products', 'STARTED');
    SET @RunID = SCOPE_IDENTITY();

    PRINT '>> Iniciando carga: silver.products';

    BEGIN TRY
        TRUNCATE TABLE silver.products;

        INSERT INTO silver.products (
            product_id,
            product_category_name,
            product_category_name_english,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm,
            created_at,
            source_system
        )
        SELECT
            TRIM(p.product_id),
            TRIM(p.product_category_name),
            t.product_category_name_english,
            TRY_CAST(p.product_name_lenght AS INT),
            TRY_CAST(p.product_description_lenght AS INT),
            TRY_CAST(p.product_photos_qty AS INT),
            TRY_CAST(REPLACE(p.product_weight_g, ',', '.') AS DECIMAL(10,2)),
            TRY_CAST(REPLACE(p.product_length_cm, ',', '.') AS DECIMAL(10,2)),
            TRY_CAST(REPLACE(p.product_height_cm, ',', '.') AS DECIMAL(10,2)),
            TRY_CAST(REPLACE(p.product_width_cm, ',', '.') AS DECIMAL(10,2)),
            SYSDATETIME(),
            'bronze.olist_products'
        FROM bronze.olist_products p
        LEFT JOIN silver.product_category_translation t
            ON TRIM(p.product_category_name) = t.product_category_name
        WHERE p.product_id IS NOT NULL
          AND TRIM(p.product_id) <> '';

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
