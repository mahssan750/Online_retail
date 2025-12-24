--Inspecting the Data & Quality Check tests
SELECT 
TABLE_SCHEMA,
TABLE_NAME,
COLUMN_NAME,
DATA_TYPE,
IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS   
--------------------------------------------------
--CHECKING FOR ROWS DUBLICATES
SELECT  
    InvoiceNo,
    InvoiceDate,
    LOWER(LTRIM(RTRIM(StockCode))) StockCode,
    LOWER(LTRIM(RTRIM([Description]))) [Description],
    Quantity,
    UnitPrice,
    TotalAmount,
    CustomerID,
    Country,
    Count(*) dublicated_rows
FROM silver.fact_main
GROUP BY InvoiceNo,
    InvoiceDate,
    StockCode,
    [Description],
    Quantity,
    UnitPrice,
    TotalAmount,
    CustomerID,
    Country
    HAVING COUNT(*) >= 2;
------------------------------------
SELECT TOP 50 *
FROM bronze.main 
-------------------------------------------------
SELECT COUNT(*) --total_rows = 541909
FROM bronze.main 
-------------------------------------------------
SELECT COUNT(main.CustomerID)
FROM bronze.main -- 406829 #Less than total rows
-------------------------------------------------
SELECT COUNT(main.Description)
FROM bronze.main -- 540455 #Less than total rows
--NULLS IN CustomerID & Description 
-------------------------------------------------
SELECT *
FROM bronze.main
WHERE Quantity < 0 --There is qty less than 0 Which is unexpected
-------------------------------------------------
SELECT COUNT(*) [qty < 0] --Count of Quantity less than 0
FROM bronze.main
WHERE Quantity < 0  -- QTY<0 = 10624 Which is unexpected > 
-- It seems that qty < 0 means Item return
-------------------------------------------------
--counting unique InvoiceNo = 25,900
SELECT  COUNT(DISTINCT InvoiceNo)
FROM silver.fact_main;

--counting unique InvoiceNo = 25,900
SELECT  COUNT(*) 
FROM silver.fact_main;
------------------------------------

SELECT TOP 1000*
FROM silver.fact_main
WHERE CustomerID IS NULL -- DIDNT GET ANY USEFULL INSIGHTS
ORDER BY InvoiceNo 

--TOTAL_ORDERS_WITHOUT_CUSTOMER_ID = 135080
SELECT COUNT(*) TOTAL_ORDERS_WITHOUT_CUSTOMER_ID
FROM silver.fact_main
WHERE CustomerID IS NULL; 

SELECT TOP 100 *
FROM silver.fact_main
WHERE InvoiceNo LIKE 'C%'; 
-- When InvoiceNo starts with 'C' QUANTITY IS LESS THAN 0 then it indicates a return of item process

SELECT TOP 100 *
FROM bronze.main
WHERE InvoiceNo LIKE 'A%'; 
-- WHEN INVOICE_NO STARTS WITH A its descriptions is  'Adjust bad debt' and CUSTOMERID IS NULL

--calculate how many returns vs how many sales transactions

--total transactions  = 541,909
SELECT COUNT(*) total_transactions 
FROM silver.fact_main;

--sales transactions  = 532,618
SELECT COUNT(*) sales_transactions
FROM silver.fact_main
WHERE InvoiceNo NOT LIKE '[A-Z]%';

--return transactions =9,288
SELECT COUNT(*) return_transactions
FROM silver.fact_main
WHERE InvoiceNo  LIKE 'C%';

--FINANCE RELATED TRANSACTIONS = 3
SELECT COUNT(*) return_transactions
FROM silver.fact_main
WHERE InvoiceNo  LIKE 'A%';
--Undefined Transactions = total - sales - return - FINANCE = 541909- 532618 -9288 -3 = 0



