/*===============================================================================
DDL Script: Create Silver Table(s) & Load data into it from bronze table(s)
===============================================================================
Script Purpose:
    This script creates table in the 'silver' schema, dropping existing table
    if IT already existS.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
--Create table silver.fact_main
IF OBJECT_ID('silver.fact_main', 'U') IS NOT NULL
    DROP TABLE silver.fact_main;
GO

CREATE TABLE silver.fact_main(
    InvoiceNo       NVARCHAR(50),
    InvoiceDate     DATETIME,
    StockCode       NVARCHAR(50),
    [Description]     NVARCHAR(255),
    Quantity        INT,
    UnitPrice       DECIMAL(10,2),
    CustomerID      NVARCHAR(50),
    Country         NVARCHAR(50)
);
GO
--------------------------------------------------------	
-- Load data into silver.fact_main from bronze.main table
--------------------------------------------------------
--Ctreate Stored Procedure: LoadSilverMain
CREATE OR ALTER PROCEDURE LoadSilverMain
AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
		PRINT '================================================';
		PRINT 'Loading silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading silver.fact_main';
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
        LOWER(LTRIM(RTRIM(InvoiceNo))) InvoiceNo,
        DATETRUNC(MINUTE, InvoiceDate) InvoiceDate,
        LOWER(LTRIM(RTRIM(StockCode))) StockCode,
        LOWER(LTRIM(RTRIM([Description]))) [Description],
        Quantity,
        UnitPrice,
        LOWER(LTRIM(RTRIM(CustomerID))) CustomerID,
        LOWER(LTRIM(RTRIM(Country))) Country,
        ROW_NUMBER() OVER(PARTITION BY --removing duplicates
                            LOWER(LTRIM(RTRIM(InvoiceNo))) ,
                            DATETRUNC(MINUTE, InvoiceDate) ,
                            LOWER(LTRIM(RTRIM(StockCode))) ,
                            LOWER(LTRIM(RTRIM([Description]))),
                            Quantity,
                            UnitPrice,
                            LOWER(LTRIM(RTRIM(CustomerID))),
                            LOWER(LTRIM(RTRIM(Country)))
                            ORDER BY
                            InvoiceDate
                            )rn
    FROM bronze.main
)t WHERE rn = 1
END

--EXEC LoadSilverMain
