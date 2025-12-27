/*========================================================
  SILVER LAYER PIPELINE
  Data Modeling Approach: Star Schema.

  Purpose:
  - Clean, deduplicate, and standardize transactional data
  - Prepare conformed dimensions
  - Build atomic fact table (invoice line grain)
========================================================*/

--------------------------------------------------------
-- Create Table: silver.fact_main
-- Purpose:
--   - Cleaned, deduplicated transactional staging table
--   - Still denormalized
--   - Source for all Silver dimensions and facts
--------------------------------------------------------
IF OBJECT_ID('silver.fact_main', 'U') IS NOT NULL
    DROP TABLE silver.fact_main;
GO

PRINT '================================================';
PRINT 'STEP 1: Creating silver.fact_main';
PRINT '================================================';

CREATE TABLE silver.fact_main(
    InvoiceNo       NVARCHAR(50),
    InvoiceDate     DATETIME2,
    StockCode       NVARCHAR(50),
    [Description]   NVARCHAR(255),
    Quantity        INT,
    UnitPrice       DECIMAL(10,2),
    TotalAmount     DECIMAL(10,2),
    CustomerID      NVARCHAR(50),
    Country         NVARCHAR(100)
);

PRINT '✓ silver.fact_main created successfully';
GO

--------------------------------------------------------
-- Load Data into silver.fact_main
-- Deduplication Strategy:
--   - ROW_NUMBER() removes exact duplicate rows
--   - Keeps the earliest occurrence per duplicate set
--------------------------------------------------------
PRINT '------------------------------------------------';
PRINT 'STEP 2: Loading data into silver.fact_main';
PRINT '------------------------------------------------';

TRUNCATE TABLE silver.fact_main;

INSERT INTO silver.fact_main (  
    InvoiceNo,
    InvoiceDate,
    StockCode,
    [Description],
    Quantity,
    UnitPrice,
    CustomerID,
    Country
)
SELECT 
    InvoiceNo,
    InvoiceDate,
    StockCode,
    [Description],
    Quantity,
    UnitPrice,
    CustomerID,
    Country 
FROM(
    SELECT 
        InvoiceNo,
        InvoiceDate,
        StockCode,
        [Description],
        Quantity,
        UnitPrice,
        CustomerID,
        Country,
        ROW_NUMBER() OVER(
            PARTITION BY 
                InvoiceNo,
                InvoiceDate,
                StockCode,
                [Description],
                Quantity,
                UnitPrice,
                CustomerID,
                Country
            ORDER BY InvoiceDate
        ) AS row_number
    FROM bronze.main
) t
WHERE row_number = 1;

PRINT '✓ Data successfully loaded into silver.fact_main';


--------------------------------------------------------
-- Create Table: silver.dim_country
-- Type: Conformed Dimension
-- Grain: One row per country
--------------------------------------------------------
IF OBJECT_ID('silver.dim_country', 'U') IS NOT NULL
    DROP TABLE silver.dim_country;
GO

PRINT '------------------------------------------------';
PRINT 'STEP 3: Creating silver.dim_country';
PRINT '------------------------------------------------';

CREATE TABLE silver.dim_country(
    country_SK   INT IDENTITY(1,1) PRIMARY KEY,
    CountryName  NVARCHAR(100)
);

ALTER TABLE silver.dim_country 
ADD CONSTRAINT UQ_dim_country UNIQUE (CountryName);

INSERT INTO silver.dim_country (CountryName)
SELECT DISTINCT Country
FROM silver.fact_main
WHERE Country IS NOT NULL;

PRINT '✓ silver.dim_country populated';


--------------------------------------------------------
-- Create Table: silver.dim_customer
-- Grain: One row per customer
-- Notes:
--   - Handles guest customers via LEFT JOIN later
--   - First/Last purchase dates derived from transactions
--------------------------------------------------------
IF OBJECT_ID('silver.dim_customer', 'U') IS NOT NULL
    DROP TABLE silver.dim_customer;
GO

PRINT '------------------------------------------------';
PRINT 'STEP 4: Creating silver.dim_customer';
PRINT '------------------------------------------------';

CREATE TABLE silver.dim_customer (
    Customer_SK        INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID         NVARCHAR(50) NOT NULL,
    FirstPurchaseDate  DATE,
    LastPurchaseDate   DATE
);

INSERT INTO silver.dim_customer (
    CustomerID,
    FirstPurchaseDate,
    LastPurchaseDate
)
SELECT 
    CustomerID,
    MIN(TRY_CONVERT(DATE, InvoiceDate)),
    MAX(TRY_CONVERT(DATE, InvoiceDate))
FROM silver.fact_main
WHERE CustomerID IS NOT NULL
GROUP BY CustomerID;

PRINT '✓ silver.dim_customer populated';


--------------------------------------------------------
-- Create Table: silver.dim_date
-- Grain: One row per calendar day
-- Date_SK format: YYYYMMDD (industry standard)
--------------------------------------------------------
IF OBJECT_ID('silver.dim_date', 'U') IS NOT NULL
    DROP TABLE silver.dim_date;
GO

PRINT '------------------------------------------------';
PRINT 'STEP 5: Creating silver.dim_date';
PRINT '------------------------------------------------';

CREATE TABLE silver.dim_date (
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

INSERT INTO silver.dim_date (
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
    FROM silver.fact_main
    WHERE InvoiceDate IS NOT NULL
) d;

PRINT '✓ silver.dim_date populated';


--------------------------------------------------------
-- Create Table: silver.dim_product
-- Grain: One row per product (StockCode + Description)
-- Note:
--   - UnitPrice intentionally excluded (transactional attribute)
--------------------------------------------------------
IF OBJECT_ID('silver.dim_product', 'U') IS NOT NULL
    DROP TABLE silver.dim_product;
GO

PRINT '------------------------------------------------';
PRINT 'STEP 6: Creating silver.dim_product';
PRINT '------------------------------------------------';

CREATE TABLE silver.dim_product (
    product_sk     INT IDENTITY(1,1) PRIMARY KEY,
    StockCode      NVARCHAR(50),
    ProductName    NVARCHAR(255),
    FirstSeenDate  DATE
);

INSERT INTO silver.dim_product (
    StockCode,
    ProductName,
    FirstSeenDate
)
SELECT
    StockCode,
    [Description],
    MIN(TRY_CONVERT(DATE, InvoiceDate))
FROM silver.fact_main
GROUP BY
    StockCode,
    [Description];

PRINT '✓ silver.dim_product populated';


--------------------------------------------------------
-- Create Table: silver.fact_sales
-- Grain:
--   ONE ROW PER INVOICE LINE
--   (Invoice × Product × Date)
--------------------------------------------------------
IF OBJECT_ID('silver.fact_sales', 'U') IS NOT NULL
    DROP TABLE silver.fact_sales;
GO

PRINT '------------------------------------------------';
PRINT 'STEP 7: Creating silver.fact_sales';
PRINT '------------------------------------------------';

CREATE TABLE silver.fact_sales(
    Sales_SK     BIGINT IDENTITY(1,1) PRIMARY KEY,
    InvoiceNo    NVARCHAR(50),   -- Degenerate dimension
    Product_SK   INT NOT NULL,
    Customer_SK  INT NULL,
    Country_SK   INT NOT NULL,
    Date_SK      INT NOT NULL,
    Quantity     INT,
    UnitPrice    DECIMAL(18,2),
    LineRevenue  DECIMAL(18,2),
    created_at   DATETIME2 DEFAULT SYSDATETIME()
);

INSERT INTO silver.fact_sales (
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
    cnty.country_SK,
    d.Date_SK,
    f.Quantity,
    f.UnitPrice,
    f.Quantity * f.UnitPrice
FROM silver.fact_main f
INNER JOIN silver.dim_product  p ON f.StockCode = p.StockCode AND f.[Description] = p.ProductName
LEFT  JOIN silver.dim_customer c ON f.CustomerID = c.CustomerID
INNER JOIN silver.dim_country  cnty ON f.Country = cnty.CountryName
INNER JOIN silver.dim_date     d ON TRY_CONVERT(DATE, f.InvoiceDate) = d.FullDate
WHERE
    f.Quantity > 0
    AND f.UnitPrice > 0
    AND f.InvoiceNo NOT LIKE 'C%';

PRINT '✓ silver.fact_sales populated';


--------------------------------------------------------
-- INDEXES
-- Purpose:
--   - Improve join and aggregation performance
--   - Optimized for star-schema query patterns
--------------------------------------------------------
PRINT '------------------------------------------------';
PRINT 'STEP 8: Creating indexes';
PRINT '------------------------------------------------';

CREATE NONCLUSTERED INDEX IX_fact_sales_Product_SK
    ON silver.fact_sales(Product_SK);

CREATE NONCLUSTERED INDEX IX_fact_sales_Customer_SK
    ON silver.fact_sales(Customer_SK);

CREATE NONCLUSTERED INDEX IX_fact_sales_Date_SK
    ON silver.fact_sales(Date_SK);

CREATE NONCLUSTERED INDEX IX_fact_sales_Country_SK
    ON silver.fact_sales(Country_SK);

CREATE NONCLUSTERED INDEX IX_dim_product_StockCode
    ON silver.dim_product(StockCode, ProductName);

CREATE NONCLUSTERED INDEX IX_dim_customer_CustomerID
    ON silver.dim_customer(CustomerID);

PRINT '✓ Indexes created successfully';

--------------------------------------------------------
-- END OF SILVER LAYER PIPELINE.
--------------------------------------------------------
