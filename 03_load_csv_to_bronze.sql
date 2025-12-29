/*******************************************************************************
 * PROCESO: Carga de archivos CSV a tablas Bronze
 * BASE DE DATOS: olist_ecommerce_dw
 *
 * PROPOSITO DEL SCRIPT:
 *   - Cargar los 9 archivos CSV del dataset Olist en las tablas Bronze.
 *   - Usar BULK INSERT para carga masiva eficiente.
 *   - Registrar cada carga en audit.ingestion_run para trazabilidad.
 *
 ******************************************************************************/

-- ============================================================================
-- 1. CREAR STORED PROCEDURE REUTILIZABLE
-- ============================================================================
CREATE OR ALTER PROCEDURE sp_bulk_load_bronze
    @FileName NVARCHAR(200),
    @TableName NVARCHAR(200),
    @UseFormatCSV BIT = 1,      -- 1 para usar FORMAT='CSV', 0 para carga normal
    @FieldTerminator NVARCHAR(10) = ',',
    @FieldQuote NVARCHAR(1) = '"'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @BasePath NVARCHAR(500) = 'C:\Users\Yamir\Documents\DWH_Olist\dataset_olist\';
    DECLARE @FullPath NVARCHAR(500) = @BasePath + @FileName;
    DECLARE @RunID BIGINT;
    DECLARE @RowCount BIGINT;
    DECLARE @StartTime DATETIME2(3) = SYSDATETIME();
    DECLARE @SQL NVARCHAR(MAX);

    PRINT '>> Cargando ' + @FileName + ' en bronze.' + @TableName + '...';

    -- Registrar inicio
    INSERT INTO audit.ingestion_run (source_system, source_object, target_schema, target_table, source_path, status)
    VALUES ('kaggle_olist', @FileName, 'bronze', @TableName, @FullPath, 'STARTED');
    SET @RunID = SCOPE_IDENTITY();

    BEGIN TRY
        -- Construir SQL dinámico
        SET @SQL = 'BULK INSERT bronze.' + @TableName + ' FROM ''' + @FullPath + ''' WITH (';

        IF @UseFormatCSV = 1
            SET @SQL = @SQL + 'FORMAT = ''CSV'', FIELDQUOTE = ''' + @FieldQuote + ''', ';

        SET @SQL = @SQL + '
            FIRSTROW = 2,
            FIELDTERMINATOR = ''' + @FieldTerminator + ''',
            ROWTERMINATOR = ''0x0a'',
            CODEPAGE = ''65001'',
            TABLOCK
        );';

        -- Ejecutar carga
        EXEC sp_executesql @SQL;
        SET @RowCount = @@ROWCOUNT;

        -- Actualizar éxito
        UPDATE audit.ingestion_run
        SET load_ended_at = SYSDATETIME(), status = 'SUCCESS', rows_inserted = @RowCount
        WHERE run_id = @RunID;

        PRINT '   OK: ' + CAST(@RowCount AS VARCHAR) + ' filas cargadas.';
    END TRY
    BEGIN CATCH
        -- Actualizar error
        UPDATE audit.ingestion_run
        SET load_ended_at = SYSDATETIME(), status = 'FAILED', error_message = ERROR_MESSAGE()
        WHERE run_id = @RunID;

        PRINT '   ERROR: ' + ERROR_MESSAGE();
    END CATCH
    PRINT '';
END;
GO

-- ============================================================================
-- 2. EJECUTAR CARGA MASIVA (SCRIPT PRINCIPAL)
-- ============================================================================

PRINT '=======================================================';
PRINT 'INICIANDO CARGA MASIVA A BRONZE';
PRINT '=======================================================';
PRINT '';

-- 1. Customers
EXEC sp_bulk_load_bronze 'olist_customers_dataset.csv', 'olist_customers';

-- 2. Sellers
EXEC sp_bulk_load_bronze 'olist_sellers_dataset.csv', 'olist_sellers';

-- 3. Products
EXEC sp_bulk_load_bronze 'olist_products_dataset.csv', 'olist_products';

-- 4. Geolocation 
EXEC sp_bulk_load_bronze 'olist_geolocation_dataset.csv', 'olist_geolocation';

-- 5. Orders
EXEC sp_bulk_load_bronze 'olist_orders_dataset.csv', 'olist_orders';

-- 6. Order Items
EXEC sp_bulk_load_bronze 'olist_order_items_dataset.csv', 'olist_order_items';

-- 7. Order Payments
EXEC sp_bulk_load_bronze 'olist_order_payments_dataset.csv', 'olist_order_payments';

-- 8. Product Category Translation
EXEC sp_bulk_load_bronze 'product_category_name_translation.csv', 'product_category_name_translation';

-- 9. Reviews (Caso especial: con |)
EXEC sp_bulk_load_bronze 
    @FileName = 'olist_order_reviews_CLEAN.csv', 
    @TableName = 'olist_order_reviews', 
    @UseFormatCSV = 0,         -- El archivo limpio ya no necesita parser CSV complejo
    @FieldTerminator = '|';    

PRINT '=======================================================';
PRINT 'CARGA COMPLETADA - RESUMEN';
PRINT '=======================================================';

SELECT target_table, status, rows_inserted, 
       DATEDIFF(SECOND, load_started_at, load_ended_at) as segs
FROM audit.ingestion_run 
WHERE load_started_at > DATEADD(MINUTE, -5, SYSDATETIME())
ORDER BY run_id DESC;
GO