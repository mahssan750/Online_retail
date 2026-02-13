1. Project Overview

The dataset contains over 500,000 retail transactions from a UK-based online retailer (2010–2011).

The data includes real-world complexities:

Duplicate rows

Cancelled invoices

Negative or zero quantities

Missing customer identifiers

Multi-country sales

Wholesale purchasing patterns

The objective is to transform raw operational data into a clean, scalable dimensional model suitable for analytics and reporting.

2. Repository Structure

The project is organized into four main SQL files:

ddl_database_and_schemas.sql
loading_bronze.sql
silver_build.sql
gold_star_schema_build.sql

2.1 ddl_database_and_schemas.sql

Creates:

Database

Schemas (bronze, silver, gold)

Purpose:
Defines the structural foundation of the warehouse.

2.2 loading_bronze.sql

Layer: 🥉 Bronze

Creates and loads:

bronze.flat_transactions_raw

Characteristics:

Raw ingestion table

No transformation logic

Preserves original structure

Acts as historical source of truth

Purpose:
Data capture and traceability.

2.3 silver_build.sql

Layer: 🥈 Silver

Creates:

silver.flat_transactions

Grain:

One row per invoice line (transaction grain)

Transformations applied:

Removes exact duplicate rows using ROW_NUMBER()

Standardizes data types

Preserves null CustomerID values

Maintains transactional structure

Purpose:
Provide a clean, reliable transaction-level dataset that feeds the Gold layer.

Silver does not apply heavy business aggregation or dimensional modeling.

2.4 gold_star_schema_build.sql

Layer: 🥇 Gold

Implements a Star Schema dimensional model.

Dimensions

gold.dim_country
One row per country.

gold.dim_customer
One row per customer (including generated guest customers).
Includes:

FirstPurchaseDate

LastPurchaseDate

Customer_Type (Retail vs Wholesale)

gold.dim_date
One row per calendar day.
Includes:

Year, Month, Quarter

WeekOfYear

DayName

Weekend flag

gold.dim_product
One row per product (StockCode + Description).
Includes first-seen date.

Fact Table

gold.fact_sales

Grain:
One row per invoice line
(Invoice × Product × Date)

Measures:

Quantity

UnitPrice

LineRevenue

Characteristics:

Uses surrogate keys

Enforces revenue logic (Quantity > 0, UnitPrice > 0)

Excludes cancelled invoices (InvoiceNo LIKE 'C%')

Optimized with analytical indexes

Purpose:
Central fact table for BI reporting and aggregation.

3. Architectural Design Principles

This project follows key warehouse best practices:

Clear separation of Bronze / Silver / Gold responsibilities

Star schema dimensional modeling

Surrogate keys in Gold

Fact table with explicit grain definition

No aggregation logic in Silver

Business logic applied in Gold

Indexed fact table for analytical performance

4. Data Flow
Raw CSV / Source
        ↓
Bronze (Raw Storage)
        ↓
Silver (Clean Transactional Layer)
        ↓
Gold (Star Schema – Analytics Ready)


Silver prepares the data.
Gold models it for analytics.

5. Intended Use

The Gold layer is designed for:

Power BI dashboards

Revenue analysis

Customer segmentation

Product performance evaluation

Time-based and seasonal analysis

All analytical queries should be executed against the Gold layer.

6. Technology Stack

SQL Server

T-SQL

Medallion Architecture

Dimensional Modeling (Star Schema)

7. Key Outcome

This repository demonstrates how to:

Transform raw retail data into a structured warehouse

Implement Medallion architecture in pure SQL

Design a proper star schema

Apply data quality rules systematically

Build an analytics-ready dimensional model

The result is a clean, scalable foundation for business intelligence and reporting.
