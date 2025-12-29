/*******************************************************************************
 * PROYECTO: Data Warehouse Olist E-commerce
 * PROCESO: QA & Validación de Datos (Post-Carga)
 * AUTOR: Yamir
 * FECHA: 2025-12-28
 ******************************************************************************/

USE olist_ecommerce_dw;
GO

PRINT '=== REPORTE DE CALIDAD DE DATOS (QA) ===';

-- ----------------------------------------------------------------------------
-- 1. CONTEOS TOTALES (Volumetría)
-- Verificar que no perdimos filas masivamente en los JOINs
-- ----------------------------------------------------------------------------
PRINT '>>> 1. Comparación Silver vs Gold (Filas)';

SELECT 'Orders' AS Entity, 
       (SELECT COUNT(*) FROM silver.orders) AS Silver_Count,
       (SELECT COUNT(*) FROM gold.fact_orders) AS Gold_Count,
       (SELECT COUNT(*) FROM gold.fact_orders) - (SELECT COUNT(*) FROM silver.orders) AS Diff;
       
SELECT 'Order Items' AS Entity, 
       (SELECT COUNT(*) FROM silver.order_items) AS Silver_Count,
       (SELECT COUNT(*) FROM gold.fact_order_items) AS Gold_Count,
       (SELECT COUNT(*) FROM gold.fact_order_items) - (SELECT COUNT(*) FROM silver.order_items) AS Diff;

-- NOTA: Si Diff < 0, significa que el INNER JOIN eliminó filas (ej. Clientes que no existen en Dim_Customer).
-- En un DW perfecto, Diff debería ser 0.

-- ----------------------------------------------------------------------------
-- 2. INTEGRIDAD DE CLAVES FORÁNEAS (Orphan Checks)
-- Verifica si hay hechos apuntando a dimensiones inexistentes (debería ser 0 por las FKs)
-- ----------------------------------------------------------------------------
PRINT '>>> 2. Integridad Referencial';

SELECT 'Orders sin Cliente' AS Check_Name, COUNT(*) AS Count_Errors
FROM gold.fact_orders f 
LEFT JOIN gold.dim_customer d ON f.customer_sk = d.customer_sk
WHERE d.customer_sk IS NULL;

-- ----------------------------------------------------------------------------
-- 3. VALIDACIÓN DE NEGOCIO (Smoke Test)
-- ¿Los números tienen sentido común?
-- ----------------------------------------------------------------------------
PRINT '>>> 3. KPIs de Sentido Común';

-- A. Ventas Totales Históricas (Debería ser ~13.59 Millones BRL aprox para Olist)
SELECT 'Ventas Totales (BRL)' AS Metric, FORMAT(SUM(total_item_value), 'C', 'pt-BR') AS Value
FROM gold.fact_order_items;

-- B. Rango de Fechas
SELECT 'Rango de Fechas' AS Metric, 
       MIN(date) AS Min_Date, 
       MAX(date) AS Max_Date
FROM gold.dim_date d
JOIN gold.fact_orders f ON f.purchase_date_key = d.date_key;

-- C. Top Categoría (Debería ser cama_mesa_banho o beleza_saude)
SELECT TOP 3 
    dp.category_name, 
    COUNT(*) AS Num_Ventas,
    FORMAT(SUM(foi.total_item_value), 'C', 'pt-BR') AS Total_Revenue
FROM gold.fact_order_items foi
JOIN gold.dim_product dp ON foi.product_sk = dp.product_sk
GROUP BY dp.category_name
ORDER BY SUM(foi.total_item_value) DESC;

-- ----------------------------------------------------------------------------
-- 4. CALIDAD DE DATOS (Nulls & Anomalías)
-- ----------------------------------------------------------------------------
PRINT '>>> 4. Anomalías';

-- ¿Cuántas órdenes NO tienen fecha de entrega? (Pendientes o Canceladas)
SELECT 'Ordenes sin entregar' AS Metric, COUNT(*) AS Qty
FROM gold.fact_orders
WHERE delivered_date_key IS NULL;

-- ¿Hay entregas con fechas negativas? (Entregado antes de comprar)
SELECT 'Fechas Imposibles' AS Metric, COUNT(*) AS Qty
FROM gold.fact_orders
WHERE total_delivery_days < 0;

PRINT '=== FIN DEL REPORTE ===';
GO
