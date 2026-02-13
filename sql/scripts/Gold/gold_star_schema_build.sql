/* ================================================================
   GOLD LAYER – ANALYTICS & PRESENTATION LAYER
   ================================================================

   Overview:
   This script builds the Gold layer star schema from the Silver 
   transactional layer. The Gold layer represents the business-ready 
   dimensional model optimized for BI reporting and analytical queries.

   Architecture:
   - Star schema design
   - Surrogate keys for all dimensions
   - Fact table at invoice-line grain
   - Business logic applied (customer segmentation)
   - Optimized for Power BI / analytical workloads

   ----------------------------------------------------------------
   Tables Created
   ----------------------------------------------------------------

   1) gold.dim_country
      - Grain: One row per country
      - Purpose: Conformed geographic dimension used for slicing sales.

   2) gold.dim_customer
      - Grain: One row per customer (including generated guest customers)
      - Purpose: Customer master dimension including:
          • FirstPurchaseDate
          • LastPurchaseDate
          • Customer_Type (Retail / Wholesale segmentation)

   3) gold.dim_date
      - Grain: One row per calendar day
      - Purpose: Standard time dimension with derived attributes 
        (Year, Month, Quarter, Week, Weekend flag).

   4) gold.dim_product
      - Grain: One row per product (StockCode + Description)
      - Purpose: Product master dimension with first-seen date.

   5) gold.fact_sales
      - Grain: One row per invoice line 
        (Invoice × Product × Date)
      - Purpose: Core transactional fact table containing:
          • Quantity
          • UnitPrice
          • LineRevenue
          • Foreign keys to all dimensions
          • InvoiceNo as degenerate dimension

   ----------------------------------------------------------------
   Data Source:
   All tables are derived from silver.flat_transactions 
   (cleaned & standardized transactional layer).

   ----------------------------------------------------------------
   Result:
   A fully functional dimensional model ready for BI reporting,
   aggregations, and advanced analytics.
================================================================ */

IF OBJECT_ID('gold.dim_country', 'U') IS NOT NULL
    DROP TABLE gold.dim_country;
GO

PRINT '------------------------------------------------';
PRINT 'STEP 1: Creating gold.dim_country';
PRINT '------------------------------------------------';

CREATE TABLE gold.dim_country(
    Country_SK   INT IDENTITY(1,1) PRIMARY KEY,
    CountryName  NVARCHAR(100) NOT NULL
);

ALTER TABLE gold.dim_country 
ADD CONSTRAINT UQ_gold_dim_country UNIQUE (CountryName);

INSERT INTO gold.dim_country (CountryName)
SELECT DISTINCT Country
FROM silver.flat_transactions
WHERE Country IS NOT NULL;

PRINT '✓ gold.dim_country populated';
--================================================================
IF OBJECT_ID('gold.dim_customer', 'U') IS NOT NULL
    DROP TABLE gold.dim_customer;
GO

PRINT '------------------------------------------------';
PRINT 'STEP 2: Creating gold.dim_customer';
PRINT '------------------------------------------------';

CREATE TABLE gold.dim_customer (
    Customer_SK        INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID         NVARCHAR(50) NOT NULL,
    FirstPurchaseDate  DATE,
    LastPurchaseDate   DATE,
    Customer_Type      NVARCHAR(50)
);

INSERT INTO gold.dim_customer (
    CustomerID,
    FirstPurchaseDate,
    LastPurchaseDate,
    Customer_Type
)
SELECT  
    CASE 
        WHEN CustomerID IS NULL 
            THEN CONCAT('GUEST_', InvoiceNo)
        ELSE CustomerID
    END AS CustomerID,

    MIN(TRY_CONVERT(DATE, InvoiceDate)),
    MAX(TRY_CONVERT(DATE, InvoiceDate)),

    CASE 
        WHEN CAST(SUM(Quantity) AS DECIMAL(18,2)) 
             / COUNT(DISTINCT InvoiceNo) >= 50 
        THEN 'Wholesale'
        ELSE 'Retail'
    END

FROM silver.flat_transactions
GROUP BY 
    CASE 
        WHEN CustomerID IS NULL 
            THEN CONCAT('GUEST_', InvoiceNo)
        ELSE CustomerID
    END;

PRINT '✓ gold.dim_customer populated';
--================================================================
IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL
    DROP TABLE gold.dim_date;
GO

PRINT '------------------------------------------------';
PRINT 'STEP 3: Creating gold.dim_date';
PRINT '------------------------------------------------';

CREATE TABLE gold.dim_date (
    Date_SK        INT PRIMARY KEY,
    FullDate       DATE,
    [Year]         INT,
    [Month]        INT,
    MonthName      NVARCHAR(20),
    Quarter        INT,
    WeekOfYear     INT,
    DayOfWeek      INT,
    DayName        NVARCHAR(20),
    IsWeekend      BIT
);

INSERT INTO gold.dim_date (
    Date_SK,
    FullDate,
    [Year],
    [Month],
    MonthName,
    Quarter,
    WeekOfYear,
    DayOfWeek,
    DayName,
    IsWeekend
)
SELECT DISTINCT
    CONVERT(INT, FORMAT(d.FullDate, 'yyyyMMdd')),
    d.FullDate,
    YEAR(d.FullDate),
    MONTH(d.FullDate),
    DATENAME(MONTH, d.FullDate),
    DATEPART(QUARTER, d.FullDate),
    DATEPART(WEEK, d.FullDate),
    DATEPART(WEEKDAY, d.FullDate),
    DATENAME(WEEKDAY, d.FullDate),
    CASE WHEN DATEPART(WEEKDAY, d.FullDate) IN (6,7) THEN 1 ELSE 0 END
FROM (
    SELECT DISTINCT TRY_CONVERT(DATE, InvoiceDate) AS FullDate
    FROM silver.flat_transactions
    WHERE InvoiceDate IS NOT NULL
) d;

PRINT '✓ gold.dim_date populated';
--================================================================
IF OBJECT_ID('gold.dim_product', 'U') IS NOT NULL
    DROP TABLE gold.dim_product;
GO

PRINT '------------------------------------------------';
PRINT 'STEP 4: Creating gold.dim_product';
PRINT '------------------------------------------------';

CREATE TABLE gold.dim_product (
    Product_SK     INT IDENTITY(1,1) PRIMARY KEY,
    StockCode      NVARCHAR(50),
    ProductName    NVARCHAR(255),
    FirstSeenDate  DATE
);

INSERT INTO gold.dim_product (
    StockCode,
    ProductName,
    FirstSeenDate
)
SELECT
    StockCode,
    [Description],
    MIN(TRY_CONVERT(DATE, InvoiceDate))
FROM silver.flat_transactions
GROUP BY
    StockCode,
    [Description];

PRINT '✓ gold.dim_product populated';
--================================================================
IF OBJECT_ID('gold.fact_sales', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales;
GO

PRINT '------------------------------------------------';
PRINT 'STEP 5: Creating gold.fact_sales';
PRINT '------------------------------------------------';

CREATE TABLE gold.fact_sales(
    Sales_SK     BIGINT IDENTITY(1,1) PRIMARY KEY,
    InvoiceNo    NVARCHAR(50),   -- Degenerate dimension
    Product_SK   INT NOT NULL,
    Customer_SK  INT NULL,
    Country_SK   INT NOT NULL,
    Date_SK      INT NOT NULL,
    Quantity     INT,
    UnitPrice    DECIMAL(18,2),
    LineRevenue  DECIMAL(18,2),
    Created_At   DATETIME2 DEFAULT SYSDATETIME()
);

INSERT INTO gold.fact_sales (
    InvoiceNo,
    Product_SK,
    Customer_SK,
    Country_SK,
    Date_SK,
    Quantity,
    UnitPrice,
    LineRevenue
)
SELECT 
    f.InvoiceNo,
    p.Product_SK,
    c.Customer_SK,
    cnty.Country_SK,
    d.Date_SK,
    f.Quantity,
    f.UnitPrice,
    f.Quantity * f.UnitPrice
FROM silver.flat_transactions f

INNER JOIN gold.dim_product  p 
    ON f.StockCode = p.StockCode 
   AND f.[Description] = p.ProductName

LEFT JOIN gold.dim_customer c 
   ON (
       CASE 
           WHEN f.CustomerID IS NULL THEN CONCAT('GUEST_', f.InvoiceNo)
           ELSE f.CustomerID
       END
   ) = c.CustomerID

INNER JOIN gold.dim_country cnty 
    ON f.Country = cnty.CountryName

INNER JOIN gold.dim_date d 
    ON TRY_CONVERT(DATE, f.InvoiceDate) = d.FullDate

WHERE
    f.Quantity > 0
    AND f.UnitPrice > 0
    AND f.InvoiceNo NOT LIKE 'C%';

PRINT '✓ gold.fact_sales populated';
--================================================================
PRINT '------------------------------------------------';
PRINT 'STEP 6: Creating indexes';
PRINT '------------------------------------------------';

CREATE NONCLUSTERED INDEX IX_gold_fact_sales_Product_SK
    ON gold.fact_sales(Product_SK);

CREATE NONCLUSTERED INDEX IX_gold_fact_sales_Customer_SK
    ON gold.fact_sales(Customer_SK);

CREATE NONCLUSTERED INDEX IX_gold_fact_sales_Date_SK
    ON gold.fact_sales(Date_SK);

CREATE NONCLUSTERED INDEX IX_gold_fact_sales_Country_SK
    ON gold.fact_sales(Country_SK);

CREATE NONCLUSTERED INDEX IX_gold_dim_product_StockCode
    ON gold.dim_product(StockCode, ProductName);

CREATE NONCLUSTERED INDEX IX_gold_dim_customer_CustomerID
    ON gold.dim_customer(CustomerID);

PRINT '✓ Indexes created successfully';
PRINT '------------------------------------------------';
PRINT 'GOLD LAYER BUILD COMPLETE';
