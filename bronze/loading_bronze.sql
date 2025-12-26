/* ================================================
    File: loading_bronze.sql
    Description: This script creates the bronze.fact_transactions_raw table and loads data from a CSV file into it.

==================================================*/
IF OBJECT_ID('bronze.fact_transactions_raw', 'U') IS NOT NULL
    DROP TABLE bronze.fact_transactions_raw;
GO

CREATE TABLE bronze.fact_transactions_raw (
    InvoiceNo       NVARCHAR(50), -- I changed INT to NVARCHAR(50) to accommodate alphanumeric invoice numbers
    StockCode       NVARCHAR(50),
    Description     NVARCHAR(255),
    Quantity        INT,
    InvoiceDate     DATETIME2,
    UnitPrice       DECIMAL(10,2),
    CustomerID      NVARCHAR(50),
    Country         NVARCHAR(50)
);
GO

--------------------------------------------------------	
-- Load data into bronze.fact_transactions_raw from CSV file
--------------------------------------------------------

CREATE OR ALTER PROCEDURE LoadBronzeMain
AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
		PRINT '================================================';
		PRINT 'Loading bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading Main Table';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.fact_transactions_raw';
		TRUNCATE TABLE bronze.fact_transactions_raw;
		PRINT '>> Inserting Data Into: bronze.fact_transactions_raw';
		BULK INSERT bronze.fact_transactions_raw
		FROM 'C:\Bulk Data\online_retail.csv'
		WITH (
            FORMAT = 'CSV',
			FIRSTROW = 2,                       -- Skip header row
			FIELDTERMINATOR = ',',              -- Adjusted for CSV format
            ROWTERMINATOR = '\n',               -- Adjusted for Windows line endings
            CODEPAGE = '65001',                 -- UTF-8 encoding
            KEEPNULLS,                     	    -- Specify how NULLs are represented                                       
			TABLOCK                             -- Use table-level lock for performance
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
	PRINT '>> Completed Loading bronze.fact_transactions_raw';
	PRINT '------------------------------------------------';
END;

--EXEC LoadBronzeMain; Uncomment to execute the procedure after creation
