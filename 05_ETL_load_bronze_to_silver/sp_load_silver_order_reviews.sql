USE olist_ecommerce_dw;
GO

CREATE OR ALTER PROCEDURE etl.sp_load_silver_order_reviews
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunID BIGINT;
    DECLARE @RowCount BIGINT;

    INSERT INTO audit.ingestion_run (source_system, source_object, target_schema, target_table, status)
    VALUES ('bronze', 'olist_order_reviews', 'silver', 'order_reviews', 'STARTED');
    SET @RunID = SCOPE_IDENTITY();

    PRINT '>> Iniciando carga: silver.order_reviews (Con Deduplicación)';

    BEGIN TRY
        TRUNCATE TABLE silver.order_reviews;

        -- 1. Definimos la CTE para calcular duplicados
        WITH ReviewsDedup AS (
            SELECT
                TRIM(review_id) AS review_id,
                TRIM(order_id) AS order_id,
                TRY_CAST(review_score AS INT) AS review_score,
                NULLIF(TRIM(review_comment_title), '') AS review_comment_title,
                NULLIF(TRIM(review_comment_message), '') AS review_comment_message,
                TRY_CONVERT(DATETIME2(3), review_creation_date) AS review_creation_date,
                TRY_CONVERT(DATETIME2(3), review_answer_timestamp) AS review_answer_timestamp,
                SYSDATETIME() AS created_at,
                'bronze.olist_order_reviews' AS source_system,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(review_id) 
                    ORDER BY TRY_CONVERT(DATETIME2(3), review_answer_timestamp) DESC
                ) AS row_num
            FROM bronze.olist_order_reviews
            WHERE review_id IS NOT NULL
              AND TRIM(review_id) <> ''
              AND order_id IS NOT NULL
              AND TRIM(order_id) <> ''
              AND TRY_CAST(review_score AS INT) BETWEEN 1 AND 5
        )
        -- 2. Insertamos SOLO las columnas reales (filtrando row_num)
        INSERT INTO silver.order_reviews (
            review_id,
            order_id,
            review_score,
            review_comment_title,
            review_comment_message,
            review_creation_date,
            review_answer_timestamp,
            created_at,
            source_system
        )
        SELECT
            review_id,
            order_id,
            review_score,
            review_comment_title,
            review_comment_message,
            review_creation_date,
            review_answer_timestamp,
            created_at,
            source_system
        FROM ReviewsDedup
        WHERE row_num = 1; 

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
