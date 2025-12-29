
USE olist_ecommerce_dw;
GO

CREATE OR ALTER PROCEDURE etl.sp_load_silver_geolocation
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunID BIGINT;
    DECLARE @RowCount BIGINT;

    INSERT INTO audit.ingestion_run (source_system, source_object, target_schema, target_table, status)
    VALUES ('bronze', 'olist_geolocation_dataset', 'silver', 'geolocation', 'STARTED');

    SET @RunID = SCOPE_IDENTITY();

    PRINT '>> Iniciando carga: silver.geolocation ';

    BEGIN TRY
        TRUNCATE TABLE silver.geolocation;

        INSERT INTO silver.geolocation (
            geolocation_zip_code_prefix,
            geolocation_city,
            geolocation_state,
            created_at,
            source_system
        )
        SELECT
            LEFT(TRIM(geolocation_zip_code_prefix), 10) AS geolocation_zip_code_prefix,
            LOWER(TRIM(geolocation_city)) COLLATE SQL_Latin1_General_CP1_CI_AI AS geolocation_city,
            UPPER(LEFT(TRIM(geolocation_state), 2)) AS geolocation_state,
            SYSDATETIME() AS created_at,
            'bronze.olist_geolocation_dataset' AS source_system
        FROM bronze.olist_geolocation
        WHERE geolocation_zip_code_prefix IS NOT NULL
          AND geolocation_city IS NOT NULL
          AND geolocation_state IS NOT NULL
        GROUP BY
            LEFT(TRIM(geolocation_zip_code_prefix), 10),
            LOWER(TRIM(geolocation_city)) COLLATE SQL_Latin1_General_CP1_CI_AI,
            UPPER(LEFT(TRIM(geolocation_state), 2));

        SET @RowCount = @@ROWCOUNT;

        UPDATE audit.ingestion_run
        SET load_ended_at = SYSDATETIME(), status = 'SUCCESS', rows_inserted = @RowCount
        WHERE run_id = @RunID;

        PRINT '   OK: ' + CAST(@RowCount AS VARCHAR(20)) + ' filas cargadas (deduplicadas).';
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

