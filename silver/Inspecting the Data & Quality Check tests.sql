--Inspecting the Data & Quality Check tests
SELECT 
TABLE_SCHEMA,
TABLE_NAME,
COLUMN_NAME,
DATA_TYPE,
IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS   
--------------------------------------------------
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
