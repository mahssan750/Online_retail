/* ================================================
    File: loading_bronze.sql
    Description: This script creates the bronze.main table and loads data from a CSV file into it.

==================================================*/
IF OBJECT_ID('bronze.main', 'U') IS NOT NULL
    DROP TABLE bronze.main;
GO

CREATE TABLE bronze.main (
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
-- Load data into bronze.main from CSV file
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
		PRINT '>> Truncating Table: bronze.main';
		TRUNCATE TABLE bronze.main;
		PRINT '>> Inserting Data Into: bronze.main';
		BULK INSERT bronze.main
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
	PRINT '>> Completed Loading bronze.main';
	PRINT '------------------------------------------------';
END;

--EXEC LoadBronzeMain; Uncomment to execute the procedure after creation
