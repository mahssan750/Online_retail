/*=====================================================
    gold.fact_product_sales
    Grain: One row per product
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
