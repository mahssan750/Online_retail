--Inspecting the Data & Quality Check tests

SELECT 
TABLE_SCHEMA,
TABLE_NAME,
COLUMN_NAME,
DATA_TYPE,
IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS   
--
SELECT TOP 50 *
FROM bronze.main 

SELECT COUNT(*) --total_rows = 541909
FROM bronze.main 

SELECT COUNT(main.CustomerID)
FROM bronze.main -- 406829 #Less than total rows

SELECT COUNT(main.Description)
FROM bronze.main -- 540455 #Less than total rows
--NULLS IN CustomerID & Description 

SELECT *
FROM bronze.main
WHERE Quantity < 0 --There is qty less than 0 Which is unexpected

SELECT COUNT(*) [qty < 0] --Count of Quantity less than 0
FROM bronze.main
WHERE Quantity < 0  -- QTY<0 = 10624 Which is unexpected > 
-- It seems that qty < 0 means Item return

--===========================================================
/*
===============================================================================
DDL Script: Create Silver Table
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
