/*==============================================================
 GOLD | Time-Based & Seasonality Analysis
==============================================================

Purpose:
--------
This script builds the Gold-layer fact table `gold.fact_time_analysis`,
which supports time-based and seasonality analytics across sales data.

The table aggregates revenue by multiple temporal dimensions to answer
key business questions such as:
- When do customers buy most frequently?
- Which months, weekdays, and hours generate the highest revenue?
- Are there identifiable seasonality or peak-time patterns?

Business Questions Answered:
----------------------------
- Revenue by month
- Revenue by day of week
- Revenue by hour of day
- Identification of peak selling periods

Source:
-------
- silver.fact_main

Grain:
------
One row per unique combination of:
- Month
- Day of week
- Hour of day

Notes:
------
- Only positive quantities and unit prices are included
  to exclude returns, cancellations, and data errors.
- This table is optimized for BI tools and exploratory analytics,
  not for transactional workloads.

==============================================================*/


--==============================================================
-- Drop existing Gold table if it exists
--==============================================================
IF OBJECT_ID('gold.fact_time_analysis', 'U') IS NOT NULL
    DROP TABLE gold.fact_time_analysis;
GO


--==============================================================
-- Create Gold time-based fact table
--==============================================================
CREATE TABLE gold.fact_time_analysis (
    [month]                 INT,            -- Calendar month number (1–12)
    [WeekDay]               INT,            -- Numeric weekday (SQL Server dependent)
    DayName                 NVARCHAR(50),    -- Weekday name (e.g., Monday)
    [Hour]                  INT,            -- Hour of day (0–23)
    total_revenue           DECIMAL(18,2)    -- Aggregated revenue for the time slice
);
GO


--==============================================================
-- Populate Gold table with aggregated time-based metrics
--==============================================================
INSERT INTO gold.fact_time_analysis (
    [month],
    [WeekDay],
    DayName,
    [Hour],
    total_revenue
)
SELECT
    MONTH(InvoiceDate)                    AS [month],       -- Extract month
    DATEPART(WEEKDAY, InvoiceDate)        AS [WeekDay],     -- Extract weekday number
    DATENAME(WEEKDAY, InvoiceDate)        AS DayName,       -- Extract weekday name
    DATEPART(HOUR, InvoiceDate)           AS [Hour],        -- Extract hour of day
    SUM(Quantity * UnitPrice)             AS total_revenue  -- Revenue aggregation
FROM silver.fact_main
WHERE 
    Quantity > 0       -- Exclude returns and negative quantities
    AND UnitPrice > 0  -- Exclude invalid or zero-priced records
GROUP BY 
    MONTH(InvoiceDate),
    DATEPART(WEEKDAY, InvoiceDate),
    DATENAME(WEEKDAY, InvoiceDate),
    DATEPART(HOUR, InvoiceDate);
GO


--==============================================================
-- Create clustered index aligned with time-based grain
--==============================================================
CREATE CLUSTERED INDEX CX_gold_fact_time_analysis_time
ON gold.fact_time_analysis ([month], [WeekDay], [Hour]);
GO
