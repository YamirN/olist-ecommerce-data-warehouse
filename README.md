# 🛒 Olist E-commerce Data Warehouse

![SQL Server](https://img.shields.io/badge/Database-SQL_Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![ETL](https://img.shields.io/badge/Pipeline-ETL-blue?style=for-the-badge)
![Power BI](https://img.shields.io/badge/BI-Power_BI-F2C811?style=for-the-badge&logo=power-bi&logoColor=black)
![Status](https://img.shields.io/badge/Status-Completed-success?style=for-the-badge)

## 📋 Project Overview
This project demonstrates an end-to-end **Data Engineering** solution for the **Olist Brazilian E-commerce** public dataset. The goal was to build a robust, scalable Data Warehouse (DWH) to analyze sales trends, delivery performance, and customer behavior.

The architecture follows the **Medallion Architecture** (Bronze → Silver → Gold) and implements a **Kimball Star Schema** optimized for BI reporting.

## 🏗️ Architecture

The ETL pipeline transforms raw CSV data into a dimensional model:

1.  **Bronze Layer (Raw):** Direct ingestion of CSV files (Data Lake approach).
2.  **Silver Layer (Cleansed):** Data cleaning, normalization (3NF), handling NULLs, and standardizing data types.
3.  **Gold Layer (Dimensional):** Final **Star Schema** with Fact and Dimension tables using Surrogate Keys (SK).

### 🌟 Data Model (Gold Schema)
*   **Facts:** `fact_orders` (Business Process), `fact_order_items` (Sales Transaction).
*   **Dimensions:** `dim_customer`, `dim_product`, `dim_seller`, `dim_date`.

## 🛠️ Tech Stack
*   **Database:** Microsoft SQL Server 2022
*   **Language:** T-SQL (Stored Procedures, Window Functions, CTEs)
*   **Modeling:** Star Schema / Dimensional Modeling
*   **Orchestration:** T-SQL Stored Procedures
*   **Visualization:** Power BI (Connected via Import Mode)


## 🚀 Key Features Implemented
*   **Surrogate Keys:** Replaced natural string IDs with optimized `INT IDENTITY` keys.
*   **Data Quality Handling:**
    *   Managed NULL dates in delivery timelines to ensure clean BI reporting.
    *   Implemented "Unknown Member" handling for referential integrity.
*   **Date Dimension:** Generated a comprehensive calendar table with T-SQL.
*   **Performance:** Proper indexing on Foreign Keys and Fact Tables.

## 📊 Results & QA
The final DWH passed all integrity checks:
*   ✅ **Revenue Match:** R$ 15.8M (Validated against source).
*   ✅ **Integrity:** 0 Orphan records in Fact tables.
*   ✅ **Data Consistency:** 0 "Impossible dates" (e.g., delivered before purchase).

## 📄 Dataset
The dataset used is the public [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) available on Kaggle.

---
*Created by [Tu Nombre] - 2025*


