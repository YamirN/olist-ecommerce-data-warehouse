
USE olist_ecommerce_dw;
GO

CREATE OR ALTER PROCEDURE etl.sp_load_silver_customers
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME2(3) = SYSDATETIME();
    DECLARE @RowCount BIGINT;
    DECLARE @RunID BIGINT;

    -- Registrar inicio en auditoría
    INSERT INTO audit.ingestion_run (source_system, source_object, target_schema, target_table, status)
    VALUES ('bronze', 'olist_customers_dataset', 'silver', 'customers', 'STARTED');
    SET @RunID = SCOPE_IDENTITY();

    PRINT '>> Iniciando carga: silver.customers';

    BEGIN TRY
        -- 1. Limpiar tabla destino
        TRUNCATE TABLE silver.customers;

        -- 2. Transformar y Cargar
        INSERT INTO silver.customers (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            created_at,
            source_system
        )
        SELECT 
            TRIM(customer_id),
            TRIM(customer_unique_id),
            LEFT(TRIM(customer_zip_code_prefix), 10),
            TRIM(customer_city),
            UPPER(LEFT(TRIM(customer_state), 2)), -- Estandarización UF
            SYSDATETIME(),
            'bronze.olist_customers_dataset'
        FROM bronze.olist_customers
        WHERE customer_id IS NOT NULL; -- Regla de calidad básica

        SET @RowCount = @@ROWCOUNT;

        -- 3. Registrar éxito
        UPDATE audit.ingestion_run
        SET load_ended_at = SYSDATETIME(),
            status = 'SUCCESS',
            rows_inserted = @RowCount
        WHERE run_id = @RunID;

        PRINT '   OK: ' + CAST(@RowCount AS VARCHAR) + ' filas cargadas.';
    END TRY
    BEGIN CATCH
        -- 4. Registrar error
        UPDATE audit.ingestion_run
        SET load_ended_at = SYSDATETIME(),
            status = 'FAILED',
            error_message = ERROR_MESSAGE()
        WHERE run_id = @RunID;

        PRINT '   ERROR: ' + ERROR_MESSAGE();
        THROW; -- Relanzar para que el orquestador sepa que falló
    END CATCH
END;
GO
