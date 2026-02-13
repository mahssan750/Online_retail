/* ================================================================
   SILVER LAYER – CLEANSED & STANDARDIZED DATA LAYER
   ================================================================

   Overview:
   This script builds the Silver layer from the Bronze raw ingestion layer.
   The Silver layer represents cleaned, deduplicated, and standardized 
   transactional data prepared for dimensional modeling.

   Architecture Role:
   - Intermediate transformation layer
   - Preserves transactional grain
   - Applies data quality rules
   - Removes duplicates
   - Standardizes data types
   - Prepares data for Gold star schema modeling

   ----------------------------------------------------------------
   Table Created
   ----------------------------------------------------------------

   1) silver.flat_transactions
      - Grain: One row per invoice line (raw transactional grain)
      - Purpose:
          • Cleaned & deduplicated version of bronze.flat_transactions_raw
          • Standardized data types
          • Removed exact duplicate rows using ROW_NUMBER()
          • Acts as the single source of truth for Gold dimensions and fact tables

      - Key Columns:
          • InvoiceNo
          • InvoiceDate
          • StockCode
          • Description
          • Quantity
          • UnitPrice
          • CustomerID
          • Country

   ----------------------------------------------------------------
   Data Source:
   bronze.flat_transactions_raw (raw ingestion layer)

   ----------------------------------------------------------------
   Result:
   A reliable, transaction-level dataset ready to feed the Gold
   dimensional model (star schema).
================================================================ */

IF OBJECT_ID('silver.flat_transactions', 'U') IS NOT NULL
    DROP TABLE silver.flat_transactions;
GO

PRINT '================================================';
PRINT 'STEP 1: Creating silver.flat_transactions';
PRINT '================================================';

CREATE TABLE silver.flat_transactions(
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

PRINT '✓ silver.flat_transactions created successfully';
GO

--------------------------------------------------------
-- Load Data into silver.flat_transactions
-- Deduplication Strategy:
--   - ROW_NUMBER() removes exact duplicate rows
--   - Keeps the earliest occurrence per duplicate set
--------------------------------------------------------
PRINT '------------------------------------------------';
PRINT 'STEP 2: Loading data into silver.flat_transactions';
PRINT '------------------------------------------------';

TRUNCATE TABLE silver.flat_transactions;

INSERT INTO silver.flat_transactions (  
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
    FROM bronze.flat_transactions_raw
) t
WHERE row_number = 1;

PRINT '✓ Data successfully loaded into silver.flat_transactions';

