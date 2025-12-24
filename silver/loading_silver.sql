--Transformation
INSERT INTO silver.fact_main
SELECT 
    InvoiceNo,
    InvoiceDate,
    StockCode,
    [Description],
    Quantity,
    UnitPrice,
    Quantity * UnitPrice AS TotalAmount,
    CustomerID,
    Country
FROM bronze.main 
WHERE CustomerID IS NOT NULL --REMOVED ORDERS WITHOUT CUSTOMERS ID FOR CUSTOMER SEGMENTAION
