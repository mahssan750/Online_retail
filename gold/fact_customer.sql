
/* ============================================================
   GOLD LAYER — CUSTOMER FACT TABLE

   This script builds a customer-level fact table (gold.fact_customer)
   from transactional data in silver.fact_main.

   Purpose:
   - Aggregate transactional sales data to one row per customer
   - Derive key customer metrics such as:
       • total orders and spend
       • quantity and product diversity
       • average order value and basket size
       • first/last purchase dates
       • customer tenure (lifetime in days)
   - Classify customers as Retail or Wholesale based on lifetime volume

   Design Notes:
   - Uses a CTE (base_query) to isolate and standardize valid transactions
   - Implements a surrogate primary key (Customer_SK) for stability
   - Intended for downstream analytics, BI dashboards, and customer
     segmentation (e.g. RFM, churn, CLV)

   Layer: GOLD (Analytics-ready)
   ============================================================ */


IF OBJECT_ID('gold.fact_customer', 'U') IS NOT NULL
    DROP TABLE gold.fact_customer;
GO

CREATE TABLE gold.fact_customer 
( 
    Customer_SK             INT IDENTITY(1,1) NOT NULL,
    CustomerID              NVARCHAR(15),
    Customer_Type           NVARCHAR(15),
    Total_Orders            INT,
    Total_Spend             DECIMAL(18,2),
    Total_Quantity_Sold     INT,
    Total_Unique_Products   INT,
    Average_Order_Value     DECIMAL(18,2),
    Average_Basket_Size     DECIMAL(18,2),
    First_Purchase_Date     DATETIME,
    Last_Purchase_Date      DATETIME,
    Customer_Tenure_Days    INT,
    CONSTRAINT PK_fact_customer PRIMARY KEY (Customer_SK)
);
GO

WITH base_query AS (
    SELECT
        CustomerID,
        InvoiceNo,
        InvoiceDate,
        Quantity,
        UnitPrice,
        [Description]
    FROM silver.fact_main
    WHERE CustomerID IS NOT NULL
      AND Quantity > 0
      AND Quantity * UnitPrice > 0
)
INSERT INTO gold.fact_customer
SELECT
    CustomerID,

    -- Wholesale classification based on lifetime quantity purchased
    CASE 
        WHEN SUM(Quantity) >= 3000 THEN 'WholeSale'
        ELSE 'Retail'
    END AS Customer_Type,

    COUNT(DISTINCT InvoiceNo) AS Total_Orders,

    SUM(Quantity * UnitPrice) AS Total_Spend,

    SUM(Quantity) AS Total_Quantity_Sold,

    COUNT(DISTINCT [Description]) AS Total_Unique_Products,

    CAST(
        SUM(Quantity * UnitPrice)
        / NULLIF(COUNT(DISTINCT InvoiceNo), 0)
        AS DECIMAL(18,2)
    ) AS Average_Order_Value,

    CAST(
        SUM(Quantity)
        / NULLIF(COUNT(DISTINCT InvoiceNo), 0)
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

--
SELECT TOP 50 *
FROM gold.fact_customer
