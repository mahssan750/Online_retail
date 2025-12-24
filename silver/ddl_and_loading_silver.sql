/*===============================================================================
DDL Script: Create Silver Table & Load data into it from bronze table
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('silver.fact_main', 'U') IS NOT NULL
    DROP TABLE silver.fact_main;
GO

CREATE TABLE silver.fact_main(
    InvoiceNo       NVARCHAR(50),
    InvoiceDate     DATETIME2,
    StockCode       NVARCHAR(50),
    [Description]     NVARCHAR(255),
    Quantity        INT,
    UnitPrice       DECIMAL(10,2),
    TotalAmount     DECIMAL(10,2),
    CustomerID      NVARCHAR(50),
    Country         NVARCHAR(50)
);
GO
--------------------------------------------------------	
-- Load data into silver.fact_main from bronze.main table
--------------------------------------------------------

CREATE OR ALTER PROCEDURE LoadSilverMain
AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
		PRINT '================================================';
		PRINT 'Loading silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading Main Table';
		PRINT '------------------------------------------------';

SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.fact_main';
		TRUNCATE TABLE silver.fact_main;
		PRINT '>> Inserting Data Into: silver.fact_main';
		INSERT INTO silver.fact_main (  
            InvoiceNo,
    InvoiceDate,
    StockCode,
    [Description],
    Quantity,
    UnitPrice,
    TotalAmount,
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
    1.00 * Quantity * UnitPrice AS TotalAmount,
    CustomerID,
    Country
FROM bronze.main
WHERE CustomerID IS NOT NULL  --REMOVED ORDERS WITHOUT CUSTOMERS ID FOR CUSTOMER SEGMENTAION
END

--EXEC LoadSilverMain
