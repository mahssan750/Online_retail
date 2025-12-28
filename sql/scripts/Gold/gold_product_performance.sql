/*=====================================================
GOLD LAYER — PRODUCT-LEVEL SALES FACT TABLE
File: gold_fact_product_sales.sql
------------
Description:
This script builds a product-level aggregated sales
fact table (gold.fact_product_sales) using the
conformed Silver star schema.
-----------
Grain:
One row per product
Purpose:
Aggregate transactional sales data by product
Enable product performance analysis for
revenue, quantity sold, pricing, and
overall contribution
Support BI dashboards and product comparisons
-----------
Key Metrics:
Total quantity sold
Total revenue
Average unit price (weighted)
Revenue contribution percentage
------------
Design Notes:
Uses surrogate keys from silver.fact_sales
(Product_SK) to ensure referential integrity
All measures are derived from validated,
line-level sales facts
Revenue contribution is calculated as a
percentage of grand total revenue across all products
Employs CTEs for aggregation and total revenue calculation
Source Tables:
silver.fact_sales
Target Table:
gold.fact_product_sales
Layer:
GOLD (Analytics-ready, aggregated data)
=====================================================*/
IF OBJECT_ID('gold.fact_product_sales', 'U') IS NOT NULL
    DROP TABLE gold.fact_product_sales;
GO

CREATE TABLE gold.fact_product_sales
(
    Product_SK                      INT NOT NULL,

    total_quantity_sold             INT,
    total_revenue                   DECIMAL(18,2),
    average_unit_price              DECIMAL(18,4),

    revenue_contribution_pct        DECIMAL(6,4),  -- e.g. 0.1234 = 12.34%

    created_at                      DATETIME2,

    CONSTRAINT PK_fact_product_sales PRIMARY KEY (Product_SK)
);
GO
/*=====================================================
    Populate gold.fact_product_sales
=====================================================*/
WITH product_agg AS
(
    SELECT
        fs.Product_SK,

        SUM(fs.Quantity) AS total_quantity_sold,

        SUM(fs.LineRevenue) AS total_revenue,

        -- Weighted average price is the ONLY correct average
        SUM(fs.LineRevenue) / NULLIF(SUM(fs.Quantity), 0) 
            AS average_unit_price

    FROM silver.fact_sales fs
    GROUP BY fs.Product_SK
),
total_revenue_cte AS
(
    SELECT
        SUM(total_revenue) AS grand_total_revenue
    FROM product_agg
)
INSERT INTO gold.fact_product_sales
SELECT
    p.Product_SK,
    p.total_quantity_sold,
    p.total_revenue,
    p.average_unit_price,

    -- TRUE revenue contribution percentage
    p.total_revenue / tr.grand_total_revenue 
        AS revenue_contribution_pct,

    SYSDATETIME() AS created_at
FROM product_agg p
CROSS JOIN total_revenue_cte tr;
GO

CREATE CLUSTERED INDEX CX_gold_fact_product_sales
ON gold.fact_product_sales (Product_SK);
