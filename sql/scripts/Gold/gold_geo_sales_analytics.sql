/*=====================================================
        gold.fact_sales_country  
=====================================================*/
IF OBJECT_ID('gold.fact_sales_country', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales_country;
GO

CREATE TABLE gold.fact_sales_country
(
    Country_SK              INT NOT NULL,   -- FK reference to dim_country
    Country                 NVARCHAR(50),
    total_orders            INT,
    total_revenue           DECIMAL(18,2),
    avg_order_value         DECIMAL(18,2),

    total_unique_customers  INT,

    total_quantity_sold     INT,
    total_unique_products   INT,

    invoice_row_count       INT,
    created_at              DATETIME2
);
GO

INSERT INTO gold.fact_sales_country
SELECT
    fs.Country_SK,
    c.CountryName,
    COUNT(DISTINCT fs.InvoiceNo) AS total_orders,

    SUM(fs.LineRevenue) AS total_revenue,

    CAST(
        SUM(fs.LineRevenue)
        / NULLIF(COUNT(DISTINCT fs.InvoiceNo), 0)
        AS DECIMAL(18,2)
    ) AS avg_order_value,

    COUNT(DISTINCT fs.Customer_SK) AS total_unique_customers,

    SUM(fs.Quantity) AS total_quantity_sold,

    COUNT(DISTINCT fs.Product_SK) AS total_unique_products,

    COUNT(*) AS invoice_row_count,

    SYSDATETIME() AS created_at

FROM silver.fact_sales fs
INNER JOIN silver.dim_country c
    ON fs.Country_SK = c.country_SK
GROUP BY
    fs.Country_SK, 
    c.CountryName;
GO

CREATE CLUSTERED INDEX CX_gold_fact_sales_country
ON gold.fact_sales_country (Country_SK);
GO

SELECT *
FROM gold.fact_sales_country
---------------------------------------------------------------
