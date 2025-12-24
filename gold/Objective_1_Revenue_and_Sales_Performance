/*=====================================================
        gold.fact_sales_daily
=====================================================*/
IF OBJECT_ID('gold.fact_sales_daily', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales_daily;
GO
CREATE TABLE gold.fact_sales_daily
(
    month_day               CHAR(7),
    month                    INT,
    day                      INT,

    total_orders            INT,
    total_revenue           DECIMAL(18,2),
    avg_order_value         DECIMAL(18,2),

    total_unique_customers  INT,
    total_countries         INT,

    total_quantity_sold     INT,
    total_unique_products   INT,

    invoice_row_count       INT,
    created_at              DATETIME
);

INSERT INTO gold.fact_sales_daily
SELECT
    -- Grain = Monthly
    FORMAT(DATETRUNC(DAY, TRY_CONVERT(DATETIME2, InvoiceDate)), 'MM-dd') AS month_day,
    MONTH(TRY_CONVERT(DATETIME2, InvoiceDate))  AS month,
    DAY(TRY_CONVERT(DATETIME2, InvoiceDate)) AS day,

    -- Orders
    COUNT(DISTINCT InvoiceNO) AS total_orders,

    -- Revenue
    SUM(Quantity * UnitPrice) AS total_revenue,

    -- AOV
    SUM(Quantity * UnitPrice) / COUNT(DISTINCT InvoiceNO) AS avg_order_value,

    -- Customers (known only)
    COUNT(DISTINCT CASE 
        WHEN CustomerID IS NOT NULL THEN CustomerID 
    END) AS total_unique_customers,

    -- Geography
    COUNT(DISTINCT Country) AS total_countries,

    -- Products / quantity
    SUM(Quantity) AS total_quantity_sold,
    COUNT(DISTINCT [Description]) AS total_unique_products,

    -- Data quality
    COUNT(*) AS invoice_row_count,
    GETDATE() AS created_at

FROM silver.fact_main
WHERE 
    InvoiceNo NOT LIKE 'c%' 
    AND LOWER(InvoiceNo) NOT LIKE 'a%' 
    AND StockCode NOT IN ('POST','DOT','M','B','AMAZONFEE')

GROUP BY 
    FORMAT(DATETRUNC(DAY, TRY_CONVERT(DATETIME2, InvoiceDate)), 'MM-dd'),
    MONTH(TRY_CONVERT(DATETIME2, InvoiceDate)),
    DAY(TRY_CONVERT(DATETIME2, InvoiceDate));
GO

CREATE CLUSTERED INDEX CX_gold_fact_sales_daily
ON gold.fact_sales_daily (month, day);
GO
SELECT *
FROM gold.fact_sales_daily



/*=====================================================
        creating Table gold.fact_sales_monthly table
=======================================================*/

IF OBJECT_ID('gold.fact_sales_monthly', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales_monthly;
GO
CREATE TABLE gold.fact_sales_monthly
(
    year_month              CHAR(7),
    year                    INT,
    month                   INT,

    total_orders            INT,
    total_revenue           DECIMAL(18,2),
    avg_order_value         DECIMAL(18,2),

    total_unique_customers  INT,
    total_countries         INT,

    total_quantity_sold     INT,
    total_unique_products   INT,

    invoice_row_count       INT,
    created_at              DATETIME
);

INSERT INTO gold.fact_sales_monthly
SELECT
    -- Grain = Monthly
    FORMAT(DATETRUNC(MONTH, TRY_CONVERT(DATETIME2, InvoiceDate)), 'yyyy-MM') AS year_month,
    YEAR(TRY_CONVERT(DATETIME2, InvoiceDate))  AS year,
    MONTH(TRY_CONVERT(DATETIME2, InvoiceDate)) AS month,

    -- Orders
    COUNT(DISTINCT InvoiceNO) AS total_orders,

    -- Revenue
    SUM(Quantity * UnitPrice) AS total_revenue,

    -- AOV
    SUM(Quantity * UnitPrice) / COUNT(DISTINCT InvoiceNO) AS avg_order_value,

    -- Customers (known only)
    COUNT(DISTINCT CASE 
        WHEN CustomerID IS NOT NULL THEN CustomerID 
    END) AS total_unique_customers,

    -- Geography
    COUNT(DISTINCT Country) AS total_countries,

    -- Products / quantity
    SUM(Quantity) AS total_quantity_sold,
    COUNT(DISTINCT [Description]) AS total_unique_products,

    -- Data quality
    COUNT(*) AS invoice_row_count,
    GETDATE() AS created_at

FROM silver.fact_main
WHERE 
    InvoiceNo NOT LIKE 'c%' 
    AND LOWER(InvoiceNo) NOT LIKE 'a%' 
    AND StockCode NOT IN ('POST','DOT','M','B','AMAZONFEE')

GROUP BY 
    FORMAT(DATETRUNC(MONTH, TRY_CONVERT(DATETIME2, InvoiceDate)), 'yyyy-MM'),
    YEAR(TRY_CONVERT(DATETIME2, InvoiceDate)),
    MONTH(TRY_CONVERT(DATETIME2, InvoiceDate));
GO

CREATE CLUSTERED INDEX CX_gold_fact_sales_monthly
ON gold.fact_sales_monthly (year, month);
GO


SELECT *
FROM gold.fact_sales_monthly
ORDER BY year, month;

-------------------------------------------------------------------------------------
/*=====================================================
            gold.fact_sales_country
=====================================================*/
IF OBJECT_ID('gold.fact_sales_country', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales_country;
GO
CREATE TABLE gold.fact_sales_country
(
    country                 CHAR(30),

    total_orders            INT,
    total_revenue           DECIMAL(18,2),
    avg_order_value         DECIMAL(18,2),

    total_unique_customers  INT,

    total_quantity_sold     INT,
    total_unique_products   INT,

    invoice_row_count       INT,
    created_at              DATETIME
);

INSERT INTO gold.fact_sales_country
SELECT
    -- Grain = Country
    DISTINCT Country,

    -- Orders
    COUNT(DISTINCT InvoiceNO) AS total_orders,

    -- Revenue
    SUM(Quantity * UnitPrice) AS total_revenue,

    -- AOV
    SUM(Quantity * UnitPrice) / COUNT(DISTINCT InvoiceNO) AS avg_order_value,

    -- Customers (known only)
    COUNT(DISTINCT CASE 
        WHEN CustomerID IS NOT NULL THEN CustomerID 
    END) AS total_unique_customers,

    -- Geography
    --COUNT(DISTINCT Country) AS total_countries,

    -- Products / quantity
    SUM(Quantity) AS total_quantity_sold,
    COUNT(DISTINCT [Description]) AS total_unique_products,

    -- Data quality
    COUNT(*) AS invoice_row_count,
    GETDATE() AS created_at

FROM silver.fact_main
WHERE 
    InvoiceNo NOT LIKE 'c%' 
    AND LOWER(InvoiceNo) NOT LIKE 'a%' 
    AND StockCode NOT IN ('POST','DOT','M','B','AMAZONFEE')

GROUP BY 
    Country
GO

CREATE CLUSTERED INDEX CX_gold_fact_sales_country
ON gold.fact_sales_country (country);
GO
SELECT *
FROM gold.fact_sales_country
---------------------------------------------------------------


