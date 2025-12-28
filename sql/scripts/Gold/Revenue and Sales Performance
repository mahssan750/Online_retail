/*=====================================================
        gold.fact_sales_daily
=====================================================*/
/* ======= gold.fact_sales_daily (star-schema aligned) ======= */
IF OBJECT_ID('gold.fact_sales_daily', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales_daily;
GO

CREATE TABLE gold.fact_sales_daily
(
    date_sk                 INT NOT NULL,   -- FK -> silver.dim_date.Date_SK
    full_date               DATE,           -- optional denormalized date for convenience

    total_orders            INT,
    total_revenue           DECIMAL(18,2),
    avg_order_value         DECIMAL(18,2),

    total_unique_customers  INT,
    total_countries         INT,

    total_quantity_sold     INT,
    total_unique_products   INT,

    invoice_row_count       INT,
    created_at              DATETIME2 DEFAULT SYSDATETIME()
);
GO

INSERT INTO gold.fact_sales_daily
(
    date_sk,
    full_date,
    total_orders,
    total_revenue,
    avg_order_value,
    total_unique_customers,
    total_countries,
    total_quantity_sold,
    total_unique_products,
    invoice_row_count,
    created_at
)
SELECT
    d.Date_SK,
    d.FullDate,

    COUNT(DISTINCT fs.InvoiceNo) AS total_orders,
    SUM(fs.LineRevenue) AS total_revenue,
    CAST(SUM(fs.LineRevenue) / NULLIF(COUNT(DISTINCT fs.InvoiceNo), 0) AS DECIMAL(18,2)) AS avg_order_value,

    COUNT(DISTINCT CASE WHEN fs.Customer_SK IS NOT NULL THEN fs.Customer_SK END) AS total_unique_customers,
    COUNT(DISTINCT fs.Country_SK) AS total_countries,

    SUM(fs.Quantity) AS total_quantity_sold,
    COUNT(DISTINCT fs.Product_SK) AS total_unique_products,

    COUNT(*) AS invoice_row_count,
    SYSDATETIME() AS created_at

FROM silver.fact_sales fs
INNER JOIN silver.dim_date d
    ON fs.Date_SK = d.Date_SK
-- optional: INNER JOIN silver.dim_product p ON fs.Product_SK = p.Product_SK -- to filter operational items if needed
WHERE 1=1
-- NOTE: filter criteria (returns / operational codes) should already be handled in silver.fact_sales.
-- If you still need to exclude certain product types, join dim_product and add a WHERE p.IsOperational = 0
GROUP BY
    d.Date_SK,
    d.FullDate
ORDER BY
    d.FullDate;
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
    year_month              CHAR(7),     -- YYYY-MM
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
    created_at              DATETIME2
);
GO

INSERT INTO gold.fact_sales_monthly
SELECT
    CONCAT(d.Year, '-', RIGHT('0' + CAST(d.Month AS VARCHAR(2)), 2)) AS year_month,
    d.Year,
    d.Month,

    COUNT(DISTINCT fs.InvoiceNo) AS total_orders,

    SUM(fs.LineRevenue) AS total_revenue,

    CAST(
        SUM(fs.LineRevenue)
        / NULLIF(COUNT(DISTINCT fs.InvoiceNo), 0)
        AS DECIMAL(18,2)
    ) AS avg_order_value,

    COUNT(DISTINCT fs.Customer_SK) AS total_unique_customers,

    COUNT(DISTINCT fs.Country_SK) AS total_countries,

    SUM(fs.Quantity) AS total_quantity_sold,

    COUNT(DISTINCT fs.Product_SK) AS total_unique_products,

    COUNT(*) AS invoice_row_count,

    SYSDATETIME() AS created_at

FROM silver.fact_sales fs
JOIN silver.dim_date d
    ON fs.Date_SK = d.Date_SK

GROUP BY
    d.Year,
    d.Month
ORDER BY
    d.Year,
    d.Month;
GO

CREATE CLUSTERED INDEX CX_gold_fact_sales_monthly
ON gold.fact_sales_monthly (year, month);
GO


