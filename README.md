# End-to-End Retail Data Warehouse  
## Medallion Architecture (Bronze → Silver → Gold)

This project implements a layered analytical data warehouse in **SQL Server** using the **Medallion Architecture** pattern.  
Raw retail transactions are transformed into a clean, scalable **star schema dimensional model** optimized for business intelligence and reporting.

---

# 1. Project Overview

The dataset contains **500,000+ retail transactions** from a UK-based online retailer (2010–2011).

The source data reflects real-world operational complexity:

- Duplicate rows  
- Cancelled invoices  
- Negative or zero quantities  
- Missing customer identifiers  
- Multi-country sales  
- Wholesale purchasing patterns  

## Objective

Transform raw operational data into a structured, analytics-ready dimensional model with clearly defined data layers and explicit table grain.
Online Retail Dataset
Transactions of a UK-based non-store online retail

## About Dataset
You can download it from Here >>
https://www.kaggle.com/datasets/ulrikthygepedersen/online-retail-dataset

**It is one flat csv table consisit of the following columns:**
-InvoiceNo: Invoice number. Nominal, a 6-digit integral number uniquely assigned to each transaction. If this code starts with letter 'c', it indicates a cancellation.
-StockCode: Product (item) code. Nominal, a 5-digit integral number uniquely assigned to each distinct product.
Description: Product (item) name. Nominal.
Quantity: The quantities of each product (item) per transaction. Numeric.
InvoiceDate: Invice Date and time. Numeric, the day and time when each transaction was generated.
UnitPrice: Unit price. Numeric, Product price per unit in sterling.
CustomerID: Customer number. Nominal, a 5-digit integral number uniquely assigned to each customer.
Country: Country name. Nominal, the name of the country where each customer resides.



---

# 2. Repository Structure

The project is organized into four core SQL scripts:

```
ddl_database_and_schemas.sql
loading_bronze.sql
silver_build.sql
gold_star_schema_build.sql
```

---

## 2.1 `ddl_database_and_schemas.sql`

**Creates:**

- Database  
- Schemas: `bronze`, `silver`, `gold`  

**Purpose:**  
Establishes the structural foundation of the warehouse.

---

## 2.2 `loading_bronze.sql`

**Layer:** 🥉 Bronze  

**Creates and Loads:**

```
bronze.flat_transactions_raw
```

### Characteristics

- Raw ingestion table  
- No transformation logic  
- Preserves original structure and values  
- Serves as the historical source of truth  

**Purpose:**  
Data capture and traceability.

---

## 2.3 `silver_build.sql`

**Layer:** 🥈 Silver  

**Creates:**

```
silver.flat_transactions
```

### Grain

One row per invoice line (transaction-level grain).

### Transformations Applied

- Removes exact duplicates using `ROW_NUMBER()`  
- Standardizes data types  
- Preserves `NULL` `CustomerID` values  
- Maintains transactional structure  

### Design Principles

The Silver layer:

- Cleans and validates data  
- Does **not** implement dimensional modeling  
- Does **not** apply business-level aggregation  

**Purpose:**  
Provide a clean, reliable transaction-level dataset that feeds the Gold layer.

---

## 2.4 `gold_star_schema_build.sql`

**Layer:** 🥇 Gold  

Implements a **Star Schema dimensional model** designed for analytics.

---

### Dimensions

#### `gold.dim_country`
- One row per country  

---

#### `gold.dim_customer`
- One row per customer (including generated guest customers)  
- Attributes:
  - `FirstPurchaseDate`
  - `LastPurchaseDate`
  - `Customer_Type` (Retail vs Wholesale)

---

#### `gold.dim_date`
- One row per calendar day  
- Attributes:
  - Year
  - Month
  - Quarter
  - WeekOfYear
  - DayName
  - Weekend flag

---

#### `gold.dim_product`
- One row per product (`StockCode` + `Description`)  
- Includes first-seen date  

---

### Fact Table

#### `gold.fact_sales`

**Grain:**  
One row per invoice line  
(Invoice × Product × Date)

### Measures

- `Quantity`  
- `UnitPrice`  
- `LineRevenue`  

### Characteristics

- Uses surrogate keys  
- Enforces revenue validity (`Quantity > 0` AND `UnitPrice > 0`)  
- Excludes cancelled invoices (`InvoiceNo LIKE 'C%'`)  
- Optimized with analytical indexes  

**Purpose:**  
Central fact table supporting BI reporting and analytical workloads.

---

# 3. Architectural Design Principles

This warehouse follows dimensional modeling best practices:

- Clear separation of Bronze / Silver / Gold responsibilities  
- Explicit fact table grain definition  
- Star schema design  
- Surrogate keys in the Gold layer  
- No aggregation logic in Silver  
- Business rules enforced in Gold  
- Indexed fact table for analytical performance  

---

# 4. Data Flow

```
Raw Source / CSV
        ↓
Bronze (Raw Storage)
        ↓
Silver (Clean Transactional Layer)
        ↓
Gold (Star Schema – Analytics Ready)
```

Silver prepares the data.  
Gold models it for analytics.

---

# 5. Intended Use

The Gold layer is designed for:

- Power BI dashboards  
- Revenue analysis  
- Customer segmentation  
- Product performance evaluation  
- Time-based and seasonal analysis  

All analytical queries should target the **Gold layer**.

---

# 6. Technology Stack

- SQL Server  
- T-SQL  
- Medallion Architecture  
- Dimensional Modeling (Star Schema)  

---

# 7. Key Outcome

This repository demonstrates how to:

- Transform raw retail data into a structured warehouse  
- Implement Medallion architecture using pure SQL  
- Design a robust star schema  
- Apply data quality rules systematically  
- Build an analytics-ready dimensional model  

The result is a clean, scalable foundation for business intelligence and reporting.
