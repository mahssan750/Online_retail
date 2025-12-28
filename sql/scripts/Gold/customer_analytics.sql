--====================================================
-- GOLD: Customer-level fact table
--====================================================
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
        fs.Quantity,
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
        WHEN SUM(Quantity) >= 3000 THEN 'Wholesale'
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
SELECT TOP 20 *
FROM gold.fact_customer
ORDER BY Total_Spend DESC;
