CREATE OR ALTER PROCEDURE etl.sp_master_orchestrator
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON; -- si algo falla, se detiene todo y hace rollback automático de la transacción actual
    
    DECLARE @MasterStart DATETIME2(3) = SYSDATETIME();
    
    PRINT '=======================================================';
    PRINT 'INICIANDO ETL BRONZE -> SILVER';
    PRINT 'Estrategia: Fail Fast (Detener si hay error)';
    PRINT '=======================================================';

    BEGIN TRY
        -- 1. Dimensiones (Críticas)
        -- Si falla Customers, NO intentar cargar Orders
        EXEC etl.sp_load_silver_customers;
        EXEC etl.sp_load_silver_sellers;
        EXEC etl.sp_load_silver_product_category_translation;
        EXEC etl.sp_load_silver_products; -- Depende de translation
        EXEC etl.sp_load_silver_geolocation;

        -- 2. Transaccionales (Dependen de dimensiones)
        EXEC etl.sp_load_silver_orders;          -- Depende de customers
        EXEC etl.sp_load_silver_order_items;     -- Depende de orders, products, sellers
        EXEC etl.sp_load_silver_order_payments;  -- Depende de orders
        EXEC etl.sp_load_silver_order_reviews;   -- Depende de orders

        PRINT '';
        PRINT '✅ ETL COMPLETADO EXITOSAMENTE';
        
    END TRY
    BEGIN CATCH
        PRINT '';
        PRINT '❌ ERROR CRÍTICO - PROCESO DETENIDO';
        PRINT 'Mensaje: ' + ERROR_MESSAGE();
        PRINT 'Paso fallido en línea: ' + CAST(ERROR_LINE() AS VARCHAR);
        
        THROW; 
    END CATCH
END;
GO
