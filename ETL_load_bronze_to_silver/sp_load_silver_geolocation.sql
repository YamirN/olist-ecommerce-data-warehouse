
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

    PRINT '>> Iniciando carga: silver.geolocation (Deduplicación)';

    BEGIN TRY
        TRUNCATE TABLE silver.geolocation;

        INSERT INTO silver.geolocation (
            geolocation_zip_code_prefix,
            geolocation_city,
            geolocation_state,
            geolocation_lat,
            geolocation_lng,
            original_row_count,
            created_at,
            source_system
        )
        SELECT 
            LEFT(TRIM(geolocation_zip_code_prefix), 10),
            TRIM(geolocation_city),
            UPPER(LEFT(TRIM(geolocation_state), 2)),
            AVG(TRY_CAST(geolocation_lat AS DECIMAL(10,6))), -- Promedio de coordenadas
            AVG(TRY_CAST(geolocation_lng AS DECIMAL(10,6))),
            COUNT(*) as original_rows,
            SYSDATETIME(),
            'bronze.olist_geolocation_dataset'
        FROM bronze.olist_geolocation
        WHERE geolocation_zip_code_prefix IS NOT NULL
        GROUP BY 
            LEFT(TRIM(geolocation_zip_code_prefix), 10),
            TRIM(geolocation_city),
            UPPER(LEFT(TRIM(geolocation_state), 2));

        SET @RowCount = @@ROWCOUNT;

        UPDATE audit.ingestion_run
        SET load_ended_at = SYSDATETIME(), status = 'SUCCESS', rows_inserted = @RowCount
        WHERE run_id = @RunID;

        PRINT '   OK: ' + CAST(@RowCount AS VARCHAR) + ' filas cargadas (Agrupadas).';
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
