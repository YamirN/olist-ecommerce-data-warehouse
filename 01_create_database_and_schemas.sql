/*******************************************************************************
 * PROYECTO: Data Warehouse Olist E-commerce (Arquitectura Medallion)
 * BASE DE DATOS: olist_ecommerce_dw
 *
 * AUTOR: YamirN
 * VERSION: 1.0
 *
 * ARQUITECTURA MEDALLION:
 *   - BRONZE: Ingesta cruda (raw) sin transformaciones desde archivos CSV
 *   - SILVER: Datos conformados, limpios y con reglas de calidad aplicadas
 *   - GOLD: Modelo dimensional (estrella) para consumo analítico/BI
 *   - AUDIT: Metadatos de carga, calidad y trazabilidad (linaje de datos)
 *
 * DATASET FUENTE: Brazilian E-Commerce Public Dataset by Olist (Kaggle)
 * URL: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
 ******************************************************************************/

USE master;
GO

-- ============================================================================
-- PASO 1: CREACIÓN DE LA BASE DE DATOS
-- ============================================================================
-- Verifica si existe antes de crear para evitar errores en re-ejecuciones
-- Se configura con RECOVERY SIMPLE para optimizar cargas masivas en desarrollo

IF DB_ID(N'olist_ecommerce_dw') IS NULL
BEGIN
    CREATE DATABASE olist_ecommerce_dw
    ON PRIMARY
    (
        NAME = N'olist_ecommerce_dw_data',
        FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\olist_ecommerce_dw_data.mdf', 
        SIZE = 512MB,           -- Tamaño inicial
        MAXSIZE = UNLIMITED,
        FILEGROWTH = 256MB      -- Crecimiento incremental
    )
    LOG ON
    (
        NAME = N'olist_ecommerce_dw_log',
        FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\olist_ecommerce_dw_log.ldf',   
        SIZE = 256MB,
        MAXSIZE = 2GB,
        FILEGROWTH = 64MB
    );

    -- Configurar modelo de recuperación simple (optimiza para ETL)
    ALTER DATABASE olist_ecommerce_dw SET RECOVERY SIMPLE;

    PRINT 'Base de datos [olist_ecommerce_dw] creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'Base de datos [olist_ecommerce_dw] ya existe. Se omite creación.';
END
GO

-- Cambiar contexto a la base recién creada
USE olist_ecommerce_dw;
GO

-- ============================================================================
-- PASO 2: CREACIÓN DE ESQUEMAS (ARQUITECTURA MEDALLION)
-- ============================================================================

/*-----------------------------------------------------------------------------
 * ESQUEMA: bronze
 * PROPOSITO: Capa de ingesta raw (landing zone) para datos sin procesar
 * USO: Cargar archivos CSV exactamente como vienen desde la fuente
 * REGLAS:
 *   - Todas las columnas como VARCHAR para evitar errores de carga
 *   - Se agrega metadata de control: ingestion_timestamp, source_file
 *   - NO se aplican reglas de negocio ni validaciones
 *   - Sirve como backup histórico y para auditoría
 * TABLAS ESPERADAS:
 *   bronze.orders_raw, bronze.order_items_raw, bronze.order_payments_raw,
 *   bronze.order_reviews_raw, bronze.customers_raw, bronze.sellers_raw,
 *   bronze.products_raw, bronze.geolocation_raw, bronze.product_category_name_translation_raw
 *---------------------------------------------------------------------------*/
IF SCHEMA_ID(N'bronze') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA bronze AUTHORIZATION dbo');
    PRINT 'Esquema [bronze] creado.';
END
GO

/*-----------------------------------------------------------------------------
 * ESQUEMA: silver
 * PROPOSITO: Capa de datos conformados y limpios (trusted zone)
 * USO: Transformación de Bronze a tipos correctos, limpieza y estandarización
 * REGLAS:
 *   - Tipos de datos SQL apropiados (INT, DATETIME2, DECIMAL, NVARCHAR)
 *   - Manejo de valores nulos según reglas de negocio
 *   - Deduplicación (especialmente geolocation)
 *   - Llaves naturales limpias y validadas
 *   - Fechas/monedas estandarizadas
 *   - Se conserva granularidad transaccional (no se agrega aún)
 * TABLAS ESPERADAS:
 *   silver.orders, silver.order_items, silver.payments, silver.reviews,
 *   silver.customers, silver.sellers, silver.products, silver.geolocation
 *---------------------------------------------------------------------------*/
IF SCHEMA_ID(N'silver') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA silver AUTHORIZATION dbo');
    PRINT 'Esquema [silver] creado.';
END
GO

/*-----------------------------------------------------------------------------
 * ESQUEMA: gold
 * PROPOSITO: Capa analítica (modelo dimensional) para consumo BI/reporting
 * USO: Data Warehouse con esquema estrella (fact + dimensions)
 * REGLAS:
 *   - Modelo estrella: tablas de hechos + dimensiones
 *   - Llaves sustitutas (surrogate keys) en dimensiones
 *   - Dimensiones SCD Tipo 1 (sobrescribir) o Tipo 2 (histórico) según caso
 *   - Tablas de hechos desnormalizadas con FK a dimensiones
 *   - Agregados pre-calculados para reportes frecuentes
 * TABLAS ESPERADAS:
 *   HECHOS: fact_sales, fact_deliveries, fact_payments, fact_reviews
 *   DIMENSIONES: dim_date, dim_customer, dim_seller, dim_product,
 *                dim_geography, dim_payment_type, dim_order_status
 *---------------------------------------------------------------------------*/
IF SCHEMA_ID(N'gold') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA gold AUTHORIZATION dbo');
    PRINT 'Esquema [gold] creado.';
END
GO

/*-----------------------------------------------------------------------------
 * ESQUEMA: audit
 * PROPOSITO: Control de procesos ETL, calidad de datos y trazabilidad
 * USO: Almacenar metadata de ejecuciones, logs de carga y métricas de calidad
 * REGLAS:
 *   - Registrar cada carga (inicio, fin, estado, filas procesadas)
 *   - Capturar errores y advertencias durante ETL
 *   - Métricas de calidad por tabla/columna
 *   - Linaje de datos (de qué fuente/carga proviene cada registro)
 * TABLAS ESPERADAS:
 *   audit.ingestion_run, audit.data_quality_checks, audit.error_log
 *---------------------------------------------------------------------------*/
IF SCHEMA_ID(N'audit') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA audit AUTHORIZATION dbo');
    PRINT 'Esquema [audit] creado.';
END
GO

/*-----------------------------------------------------------------------------
 * ESQUEMA: etl
 * PROPOSITO: Centralizar la lógica de transformación y orquestación (ELT/ETL)
 * USO: Contenedor de Stored Procedures para mover y limpiar datos entre capas
 * REGLAS DE DISEÑO:
 *   - Modularidad: Un Stored Procedure (SP) específico por cada tabla destino
 *   - Idempotencia: Los procesos deben ser re-ejecutables (TRUNCATE/INSERT o MERGE)
 *   - Trazabilidad: Cada SP debe invocar a [audit].ingestion_run para registrar actividad
 *   - Robustez: Uso obligatorio de bloques TRY/CATCH para manejo de errores
 * OBJETOS ESPERADOS:
 *   - SPs de Carga: etl.sp_load_silver_*, etl.sp_load_gold_*
 *   - Orquestador: etl.sp_master_orchestrator
 *---------------------------------------------------------------------------*/
IF SCHEMA_ID(N'etl') IS NULL
BEGIN 
	EXEC (N'CREATE SCHEMA etl AUTHORIZATION dbo');
	PRINT 'Esquema [etl] creado.';
END
GO



