# 🛒 Data Warehouse de E-commerce (Olist)

![SQL Server](https://img.shields.io/badge/Database-SQL_Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![ETL](https://img.shields.io/badge/Pipeline-ETL-blue?style=for-the-badge)
![Status](https://img.shields.io/badge/Estado-Completado-success?style=for-the-badge)

## 📌 Descripción del proyecto
Este proyecto implementa una solución End-to-End de **Data Engineering** para el dataset público de e-commerce brasileño **Olist**. El objetivo es construir un Data Warehouse listo para BI, con métricas de ventas y logística (entregas), y un modelo dimensional optimizado para reporting.  
El modelo final sigue un enfoque de **Star Schema** recomendado para rendimiento y usabilidad en herramientas como Power BI. [web:240]

## 🏗️ Arquitectura (Bronze → Silver → Gold)
- **Bronze (Raw):** Ingesta directa de CSV (datos crudos).
- **Silver (Cleansed):** Limpieza, tipado, normalización y reglas de calidad (manejo de NULLs y consistencia).
- **Gold (Dimensional):** Modelo dimensional tipo estrella con **Surrogate Keys** y tablas de hechos/dimensiones.

## 🌟 Modelo dimensional (Gold)
**Hechos**
- `fact_orders`: Métricas a nivel pedido (tiempos de entrega, retrasos, estado).
- `fact_order_items`: Métricas a nivel ítem (precio, flete, total).
- `fact_reviews`: Métricas a nivel de pedidos(Comentarios, Calificaciones buenas y malas).

**Dimensiones**
- `dim_customer`, `dim_product`, `dim_seller`, `dim_date`.

> Nota: Se permitió el uso de **NULLs reales** en fechas opcionales (por ejemplo, entrega no disponible) para evitar “fechas dummy” que confunden el análisis y los ejes temporales en BI. [web:246][web:258]

## 🧰 Tecnologías usadas
- **Base de datos:** Microsoft SQL Server
- **Lenguaje:** T‑SQL (Stored Procedures, CTEs, DDL/DML)
- **Modelado:** Dimensional Modeling (Star Schema)
- **Visualización:** Power BI (modo Import recomendado para este volumen). [web:240]


## ✅ QA / Validaciones
Se incluyeron scripts de validación para:
- Comparación de volumetría (Silver vs Gold)
- Integridad referencial (orphans = 0)
- Anomalías (fechas imposibles, nulos esperados)

## 📄 Dataset
Dataset público en Kaggle:  
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

---




