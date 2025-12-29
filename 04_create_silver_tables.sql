
/*******************************************************************************
 * CAPA: Silver (Trusted / Cleansed Layer)
 *
 * PROPOSITO DEL SCRIPT:
 *   - Crear las tablas de la capa Silver que recibirán datos transformados
 *     desde Bronze, aplicando:
 *       • Conversión de tipos de datos a formatos nativos SQL
 *       • Limpieza y estandarización de valores
 *       • Deduplicación (especialmente en geolocation)
 *       • Validación de integridad referencial
 *       • Manejo de valores nulos según reglas de negocio
 *
 * REGLAS DE TRANSFORMACIÓN BRONZE → SILVER:
 *   1. IDs: VARCHAR(50) → VARCHAR(50) (se mantienen como UUID string)
 *   2. Fechas: VARCHAR(50) → DATETIME2(3) usando TRY_CONVERT
 *   3. Montos: VARCHAR → DECIMAL(10,2)
 *   4. Enteros: VARCHAR → INT
 *   5. Textos: VARCHAR → NVARCHAR (soporte Unicode/acentos)
 *   6. NULLs: Se definen columnas NOT NULL solo donde corresponda
 *
 * TRANSFORMACIONES ESPECIALES:
 *   - geolocation: deduplicar por zip_code_prefix (promedio lat/lng)
 *   - orders: calcular delivery_days, status_flag, etc.
 *   - payments: sumar payment_value por order para total_payment
 *
 ******************************************************************************/

USE olist_ecommerce_dw;
GO

-- ============================================================================
-- LIMPIEZA PREVIA: ELIMINAR TABLAS SILVER (SI EXISTEN)
-- ============================================================================
-- Permite re-ejecutar el script durante desarrollo sin conflictos de objetos

DROP TABLE IF EXISTS silver.order_reviews;
DROP TABLE IF EXISTS silver.order_payments;
DROP TABLE IF EXISTS silver.order_items;
DROP TABLE IF EXISTS silver.orders;
DROP TABLE IF EXISTS silver.customers;
DROP TABLE IF EXISTS silver.sellers;
DROP TABLE IF EXISTS silver.products;
DROP TABLE IF EXISTS silver.geolocation;
DROP TABLE IF EXISTS silver.product_category_translation;
GO

PRINT '========================================================================';
PRINT 'CREANDO TABLAS SILVER';
PRINT '========================================================================';
PRINT '';

-- ============================================================================
-- TABLA: silver.customers
-- FUENTE: bronze.olist_customers_dataset
-- DESCRIPCIÓN:
--   Clientes del marketplace Olist con ubicación geográfica estandarizada.
-- TRANSFORMACIONES APLICADAS:
--   - customer_zip_code_prefix: VARCHAR(50) → VARCHAR(10), limpieza de espacios
--   - customer_city/state: VARCHAR → NVARCHAR (soporte acentos brasileños)
--   - Validación: rechazar filas con customer_id NULL
-- GRANULARIDAD:
--   1 fila = 1 customer_id (PK)
-- NOTAS:
--   - customer_unique_id permite identificar clientes recurrentes
--   - customer_state es UF (2 letras): SP, RJ, MG, etc.
-- ============================================================================

CREATE TABLE silver.customers (
    customer_id              VARCHAR(50)   NOT NULL,  -- PK, UUID
    customer_unique_id       VARCHAR(50)   NOT NULL,  -- ID real del cliente
    customer_zip_code_prefix VARCHAR(10)   NULL,      -- CEP Brasil (5 dígitos)
    customer_city            NVARCHAR(100) NULL,      -- Permite acentos
    customer_state           VARCHAR(2)    NULL,      -- UF: SP, RJ, MG, etc.

    -- Auditoría y linaje
    created_at               DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),
    updated_at               DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),
    source_system            VARCHAR(50)   NOT NULL DEFAULT 'bronze.olist_customers_dataset',

    CONSTRAINT PK_customers PRIMARY KEY (customer_id)
);
GO
 
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Clientes del marketplace Olist (capa Silver). Datos limpios con tipos nativos SQL y soporte Unicode. customer_id es PK técnica por orden, customer_unique_id identifica al cliente real.',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'customers';
GO

PRINT '✓ Tabla silver.customers creada.';

-- ============================================================================
-- TABLA: silver.sellers
-- FUENTE: bronze.olist_sellers_dataset
-- DESCRIPCIÓN:
--   Vendedores del marketplace con ubicación geográfica.
-- TRANSFORMACIONES APLICADAS:
--   - seller_zip_code_prefix: VARCHAR(50) → VARCHAR(10)
--   - seller_city: VARCHAR → NVARCHAR (acentos)
--   - seller_state: VARCHAR(5) → VARCHAR(2) (UF estándar)
-- GRANULARIDAD:
--   1 fila = 1 seller_id (PK)
-- ============================================================================

CREATE TABLE silver.sellers (
    seller_id              VARCHAR(50)   NOT NULL,  -- PK, UUID
    seller_zip_code_prefix VARCHAR(10)   NULL,
    seller_city            NVARCHAR(100) NULL,
    seller_state           VARCHAR(2)    NULL,      -- UF: SP, RJ, MG

    -- Auditoría
    created_at             DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),
    updated_at             DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),
    source_system          VARCHAR(50)   NOT NULL DEFAULT 'bronze.olist_sellers_dataset',

    CONSTRAINT PK_sellers PRIMARY KEY (seller_id)
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Vendedores del marketplace Olist (Silver). Contiene ubicación geográfica para análisis de distancia seller-customer y tiempos de envío.',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'sellers';
GO

PRINT '✓ Tabla silver.sellers creada.';

-- ============================================================================
-- TABLA: silver.product_category_translation
-- FUENTE: bronze.product_category_name_translation
-- DESCRIPCIÓN:
--   Diccionario de traducción portugués → inglés de categorías de productos.
-- TRANSFORMACIONES APLICADAS:
--   - TRIM() para eliminar espacios
--   - UPPER() para estandarizar búsquedas (opcional)
-- GRANULARIDAD:
--   1 fila = 1 categoría (PK: product_category_name)
-- ============================================================================

CREATE TABLE silver.product_category_translation (
    product_category_name         NVARCHAR(100) NOT NULL,  -- PK, en portugués
    product_category_name_english NVARCHAR(100) NOT NULL,  -- Traducción inglés

    -- Auditoría
    created_at                    DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),
    source_system                 VARCHAR(50)   NOT NULL DEFAULT 'bronze.product_category_name_translation',

    CONSTRAINT PK_product_category_translation PRIMARY KEY (product_category_name)
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Diccionario de traducción de categorías producto (portugués → inglés). Se usa para enriquecer reportes en Gold con nombres en inglés.',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'product_category_translation';
GO

PRINT '✓ Tabla silver.product_category_translation creada.';

-- ============================================================================
-- TABLA: silver.products
-- FUENTE: bronze.olist_products_dataset
-- DESCRIPCIÓN:
--   Catálogo de productos con atributos físicos (peso, dimensiones) y categoría.
-- TRANSFORMACIONES APLICADAS:
--   - product_name_lenght: VARCHAR → INT (conversión con TRY_CAST)
--   - product_description_lenght: VARCHAR → INT
--   - product_photos_qty: VARCHAR → INT
--   - product_weight_g: VARCHAR → DECIMAL(10,2)
--   - product_*_cm: VARCHAR → DECIMAL(10,2)
--   - JOIN con product_category_translation para obtener categoría en inglés
-- GRANULARIDAD:
--   1 fila = 1 product_id (PK)
-- NOTA:
--   - Se mantiene el typo "lenght" del CSV original por trazabilidad
-- ============================================================================

CREATE TABLE silver.products (
    product_id                      VARCHAR(50)    NOT NULL,  -- PK, UUID
    product_category_name           NVARCHAR(100)  NULL,      -- Categoría portugués
    product_category_name_english   NVARCHAR(100)  NULL,      -- Categoría inglés (desde translation)
    product_name_length             INT            NULL,      -- Largo nombre (caracteres)
    product_description_length      INT            NULL,      -- Largo descripción
    product_photos_qty              INT            NULL,      -- Cantidad fotos
    product_weight_g                DECIMAL(10,2)  NULL,      -- Peso gramos
    product_length_cm               DECIMAL(10,2)  NULL,      -- Largo cm
    product_height_cm               DECIMAL(10,2)  NULL,      -- Alto cm
    product_width_cm                DECIMAL(10,2)  NULL,      -- Ancho cm

    -- Columnas calculadas
    product_volume_cm3              AS (product_length_cm * product_height_cm * product_width_cm) PERSISTED,

    -- Auditoría
    created_at                      DATETIME2(3)   NOT NULL DEFAULT SYSDATETIME(),
    updated_at                      DATETIME2(3)   NOT NULL DEFAULT SYSDATETIME(),
    source_system                   VARCHAR(50)    NOT NULL DEFAULT 'bronze.olist_products_dataset',

    CONSTRAINT PK_products PRIMARY KEY (product_id)
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Catálogo de productos (Silver). Contiene dimensiones físicas (peso, volumen) y categoría bilingüe. product_volume_cm3 es columna calculada para análisis logístico.',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'products';
GO

PRINT '✓ Tabla silver.products creada.';

-- ============================================================================
-- TABLA: silver.geolocation
-- FUENTE: bronze.olist_geolocation_dataset
-- DESCRIPCIÓN:
--   Coordenadas geográficas deduplicadas por código postal (CEP).
-- TRANSFORMACIONES APLICADAS:
--   - Deduplicación: 1 fila por zip_code_prefix (promedio lat/lng si hay duplicados)
--   - geolocation_lat/lng: VARCHAR → DECIMAL(10,6) (precisión GPS)
--   - Validación: rechazar lat/lng fuera de rango Brasil (-34 a 5 lat, -74 a -34 lng)
-- GRANULARIDAD:
--   1 fila = 1 zip_code_prefix (PK compuesta con city/state por seguridad)
-- NOTA:
--   - Bronze tiene ~1M filas, Silver tendrá ~19k (una por CEP único)
-- ============================================================================

CREATE TABLE silver.geolocation (
    geolocation_zip_code_prefix VARCHAR(10)    NOT NULL,  -- CEP (5 dígitos)
    geolocation_city            NVARCHAR(100)  NOT NULL,
    geolocation_state           VARCHAR(2)     NOT NULL,  -- UF
    geolocation_lat             DECIMAL(10,6)  NOT NULL,  -- Latitud (promedio si duplicados)
    geolocation_lng             DECIMAL(10,6)  NOT NULL,  -- Longitud (promedio)

    -- Metadata de deduplicación
    original_row_count          INT            NULL,      -- Cuántas filas Bronze se agruparon

    -- Auditoría
    created_at                  DATETIME2(3)   NOT NULL DEFAULT SYSDATETIME(),
    source_system               VARCHAR(50)    NOT NULL DEFAULT 'bronze.olist_geolocation_dataset',

    CONSTRAINT PK_geolocation PRIMARY KEY (geolocation_zip_code_prefix, geolocation_city, geolocation_state)
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Coordenadas geográficas deduplicadas por CEP (Silver). Se promedian lat/lng de duplicados en Bronze. original_row_count indica cuántas filas se agruparon por zip.',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'geolocation';
GO

PRINT '✓ Tabla silver.geolocation creada.';

-- ============================================================================
-- TABLA: silver.orders
-- FUENTE: bronze.olist_orders_dataset
-- DESCRIPCIÓN:
--   Órdenes/pedidos con estado y timestamps del ciclo de vida completo.
-- TRANSFORMACIONES APLICADAS:
--   - Todas las fechas: VARCHAR(50) → DATETIME2(3) con TRY_CONVERT
--   - order_status: estandarización UPPER/TRIM
--   - Validación: order_purchase_timestamp NOT NULL
-- COLUMNAS CALCULADAS:
--   - delivery_days: días entre order_purchase y order_delivered_customer
--   - delay_days: días de retraso vs. estimated_delivery_date
--   - is_delivered: flag 1/0 si order_status = 'DELIVERED'
-- GRANULARIDAD:
--   1 fila = 1 order_id (PK)
-- ============================================================================

CREATE TABLE silver.orders (
    order_id                        VARCHAR(50)   NOT NULL,  -- PK, UUID
    customer_id                     VARCHAR(50)   NOT NULL,  -- FK a customers
    order_status                    VARCHAR(20)   NOT NULL,  -- delivered, shipped, canceled, etc.
    order_purchase_timestamp        DATETIME2(3)  NOT NULL,  -- Fecha compra (obligatoria)
    order_approved_at               DATETIME2(3)  NULL,
    order_delivered_carrier_date    DATETIME2(3)  NULL,      -- Entrega a transportista
    order_delivered_customer_date   DATETIME2(3)  NULL,      -- Entrega a cliente
    order_estimated_delivery_date   DATETIME2(3)  NULL,      -- Fecha estimada

    -- Columnas calculadas (métricas de negocio)
    delivery_days                   AS DATEDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date) PERSISTED,
    delay_days                      AS DATEDIFF(DAY, order_estimated_delivery_date, order_delivered_customer_date) PERSISTED,
    is_delivered                    AS CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END PERSISTED,

    -- Auditoría
    created_at                      DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),
    updated_at                      DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),
    source_system                   VARCHAR(50)   NOT NULL DEFAULT 'bronze.olist_orders_dataset',

    CONSTRAINT PK_orders PRIMARY KEY (order_id)
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Órdenes (Silver). Contiene ciclo completo de timestamps y columnas calculadas: delivery_days (tiempo total entrega), delay_days (retraso vs estimado), is_delivered (flag binario).',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'orders';
GO

PRINT '✓ Tabla silver.orders creada.';

-- ============================================================================
-- TABLA: silver.order_items
-- FUENTE: bronze.olist_order_items_dataset
-- DESCRIPCIÓN:
--   Líneas de detalle de productos vendidos por orden.
-- TRANSFORMACIONES APLICADAS:
--   - order_item_id: VARCHAR → INT
--   - shipping_limit_date: VARCHAR → DATETIME2(3)
--   - price: VARCHAR → DECIMAL(10,2)
--   - freight_value: VARCHAR → DECIMAL(10,2)
-- COLUMNAS CALCULADAS:
--   - total_item_value: price + freight_value (valor total del item)
-- GRANULARIDAD:
--   1 fila = 1 item dentro de una orden (PK: order_id + order_item_id)
-- ============================================================================

CREATE TABLE silver.order_items (
    order_id            VARCHAR(50)   NOT NULL,  -- FK a orders
    order_item_id       INT           NOT NULL,  -- Secuencial dentro de orden
    product_id          VARCHAR(50)   NOT NULL,  -- FK a products
    seller_id           VARCHAR(50)   NOT NULL,  -- FK a sellers
    shipping_limit_date DATETIME2(3)  NULL,
    price               DECIMAL(10,2) NOT NULL,  -- Precio unitario
    freight_value       DECIMAL(10,2) NOT NULL,  -- Costo envío del item

    -- Columna calculada
    total_item_value    AS (price + freight_value) PERSISTED,

    -- Auditoría
    created_at          DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),
    source_system       VARCHAR(50)   NOT NULL DEFAULT 'bronze.olist_order_items_dataset',

    CONSTRAINT PK_order_items PRIMARY KEY (order_id, order_item_id)
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Items vendidos por orden (Silver). total_item_value = price + freight_value. PK compuesta: order_id + order_item_id.',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'order_items';
GO

PRINT '✓ Tabla silver.order_items creada.';

-- ============================================================================
-- TABLA: silver.order_payments
-- FUENTE: bronze.olist_order_payments_dataset
-- DESCRIPCIÓN:
--   Transacciones de pago por orden (una orden puede tener múltiples pagos).
-- TRANSFORMACIONES APLICADAS:
--   - payment_sequential: VARCHAR → INT
--   - payment_installments: VARCHAR → INT
--   - payment_value: VARCHAR → DECIMAL(10,2)
--   - payment_type: estandarización LOWER/TRIM
-- GRANULARIDAD:
--   1 fila = 1 pago (PK: order_id + payment_sequential)
-- ============================================================================

CREATE TABLE silver.order_payments (
    order_id             VARCHAR(50)   NOT NULL,  -- FK a orders
    payment_sequential   INT           NOT NULL,  -- Secuencial de pago
    payment_type         VARCHAR(20)   NOT NULL,  -- credit_card, boleto, voucher, debit_card
    payment_installments INT           NOT NULL,  -- Número de cuotas (1 = pago único)
    payment_value        DECIMAL(10,2) NOT NULL,  -- Monto del pago

    -- Auditoría
    created_at           DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),
    source_system        VARCHAR(50)   NOT NULL DEFAULT 'bronze.olist_order_payments_dataset',

    CONSTRAINT PK_order_payments PRIMARY KEY (order_id, payment_sequential)
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Pagos por orden (Silver). payment_type: credit_card, boleto (efectivo Brasil), voucher, debit_card. payment_installments indica cuotas (común en Brasil pagar en 3-12 cuotas).',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'order_payments';
GO

PRINT '✓ Tabla silver.order_payments creada.';

-- ============================================================================
-- TABLA: silver.order_reviews
-- FUENTE: bronze.olist_order_reviews_dataset
-- DESCRIPCIÓN:
--   Calificaciones y comentarios de clientes sobre órdenes entregadas.
-- TRANSFORMACIONES APLICADAS:
--   - review_score: VARCHAR → INT (1-5)
--   - review_creation_date: VARCHAR → DATETIME2(3)
--   - review_answer_timestamp: VARCHAR → DATETIME2(3)
--   - review_comment_title/message: VARCHAR → NVARCHAR (acentos), NULL si vacío
-- GRANULARIDAD:
--   1 fila = 1 review (PK: review_id, único por order_id)
-- NOTA:
--   - Muchos reviews tienen solo score sin comentario (NULL en title/message)
-- ============================================================================

CREATE TABLE silver.order_reviews (
    review_id               VARCHAR(50)    NOT NULL,  -- PK, UUID
    order_id                VARCHAR(50)    NOT NULL,  -- FK a orders (único)
    review_score            INT            NOT NULL,  -- 1-5 estrellas
    review_comment_title    NVARCHAR(500)  NULL,      -- Título comentario (puede ser NULL)
    review_comment_message  NVARCHAR(MAX)  NULL,      -- Texto comentario (puede ser NULL)
    review_creation_date    DATETIME2(3)   NULL,
    review_answer_timestamp DATETIME2(3)   NULL,      -- Respuesta del vendedor (poco común)

    -- Flags de negocio
    has_comment             AS CASE WHEN review_comment_message IS NOT NULL THEN 1 ELSE 0 END PERSISTED,
    is_promoter             AS CASE WHEN review_score >= 4 THEN 1 ELSE 0 END PERSISTED,  -- NPS: promotor
    is_detractor            AS CASE WHEN review_score <= 2 THEN 1 ELSE 0 END PERSISTED,  -- NPS: detractor

    -- Auditoría
    created_at              DATETIME2(3)   NOT NULL DEFAULT SYSDATETIME(),
    source_system           VARCHAR(50)    NOT NULL DEFAULT 'bronze.olist_order_reviews_dataset',

    CONSTRAINT PK_order_reviews PRIMARY KEY (review_id)
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Reviews de clientes (Silver). Incluye flags NPS: is_promoter (score 4-5), is_detractor (score 1-2). has_comment indica si dejó texto. Máximo 1 review por order_id.',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'TABLE',  @level1name = N'order_reviews';
GO

PRINT '✓ Tabla silver.order_reviews creada.';

