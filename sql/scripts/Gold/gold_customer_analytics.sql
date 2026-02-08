/*=====================================================
  GOLD LAYER — CUSTOMER ANALYTICS FACT TABLE
  File: gold_fact_customer.sql

  Description:
  This script builds a customer-level fact table
  (gold.fact_customer) using the conformed Silver
  star schema.

  Grain:
  - One row per customer (CustomerID)

  Purpose:
  - Aggregate transactional sales data to the
    customer level
  - Produce analytics-ready customer KPIs for
    reporting, segmentation, and BI use cases

  Key Metrics:
  - Total orders and total spend
  - Total quantity purchased
  - Unique products purchased
  - Average Order Value (AOV)
  - Average Basket Size
  - First and last purchase dates
  - Customer tenure (lifetime in days)

  Business Logic:
  - Customer classification into Retail vs Wholesale
    based on lifetime quantity purchased
  - Uses surrogate keys from silver.fact_sales
    and conformed dimensions (date, customer)

  Source Tables:
  - silver.fact_sales
  - silver.dim_customer
  - silver.dim_date

  Target Table:
  - gold.fact_customer

  Layer:
  - GOLD (Analytics-ready, aggregated data)

=====================================================*/

IF OBJECT_ID('gold.fact_customer', 'U') IS NOT NULL
    DROP TABLE gold.fact_customer;
GO

CREATE TABLE gold.fact_customer 
( 
    Customer_SK             INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID              NVARCHAR(50),
    Customer_Type           NVARCHAR(15),
    Total_Orders            INT,
    Total_Spend             DECIMAL(18,2),
    Total_Quantity_Sold     INT,
    Total_Unique_Products   INT,
    Average_Order_Value     DECIMAL(18,2),
    Average_Basket_Size     DECIMAL(18,2),
    First_Purchase_Date     DATE,
    Last_Purchase_Date      DATE,
    Customer_Tenure_Days    INT
);
GO
WITH base_query AS (
    SELECT
        c.CustomerID,
        fs.InvoiceNo,
        d.FullDate      AS InvoiceDate,
        fs.Quantity,    --from silver.fact_sales -grain is at ONE ROW PER INVOICE LINE (Invoice × Product × Date)
        fs.LineRevenue,
        fs.Product_SK
    FROM silver.fact_sales fs
    INNER JOIN silver.dim_customer c
        ON fs.Customer_SK = c.Customer_SK
    INNER JOIN silver.dim_date d
        ON fs.Date_SK = d.Date_SK
)

INSERT INTO gold.fact_customer
SELECT
    CustomerID,

    -- Customer classification
    CASE 
        WHEN SUM(Quantity) / COUNT(DISTINCT InvoiceNo) >= 50 THEN 'Wholesale'
        ELSE 'Retail'
    END AS Customer_Type,

    COUNT(DISTINCT InvoiceNo) AS Total_Orders,

    SUM(LineRevenue) AS Total_Spend,

    SUM(Quantity) AS Total_Quantity_Sold,

    COUNT(DISTINCT Product_SK) AS Total_Unique_Products,

    CAST(
        SUM(LineRevenue) / NULLIF(COUNT(DISTINCT InvoiceNo), 0)
        AS DECIMAL(18,2)
    ) AS Average_Order_Value,

    CAST(
        SUM(Quantity) / NULLIF(COUNT(DISTINCT InvoiceNo), 0)
        AS DECIMAL(18,2)
    ) AS Average_Basket_Size,

    MIN(InvoiceDate) AS First_Purchase_Date,
    MAX(InvoiceDate) AS Last_Purchase_Date,

    DATEDIFF(
        DAY,
        MIN(InvoiceDate),
        MAX(InvoiceDate)
    ) AS Customer_Tenure_Days

FROM base_query
GROUP BY CustomerID;

----
--SELECT TOP 20 *
--FROM gold.fact_customer
--ORDER BY Total_Spend DESC;
