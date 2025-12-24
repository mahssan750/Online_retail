# End-to-End Analytical Data Pipeline: Bronze–Silver–Gold Architecture for Retail Transactional Data

This project implements a medallion architecture (Bronze → Silver → Gold) for processing retail transactional data, designed to support business reporting, analytics, and decision-making. The entire pipeline is built using T-SQL for scalability and integration with SQL-based systems.

## 1. Project Context (Problem Framing)

This project analyzes a large-scale transactional dataset from a UK-based online retailer operating between December 2010 and December 2011. The dataset represents real-world retail complexity, including:

- High transaction volume
- Mixed product types (physical items, postage, fees, manual adjustments)
- Cancellations and corrections
- Wholesale behavior (large quantities, high invoice values)
- Missing customer identifiers
- Multi-country sales

The objective is to transform raw transactional data into business-ready analytical models using a medallion architecture (Bronze → Silver → Gold), implemented entirely in T-SQL.

## 2. Overall Project Objective (High-Level)

To design and implement a scalable Gold Layer in T-SQL that converts cleaned transactional data into business-ready analytical tables, enabling revenue analysis, customer behavior analysis, product performance evaluation, and time-based insights.

## 3. What the Gold Layer Represents (Conceptual)

The Gold Layer should be treated as:

- No raw fields
- No row-level noise
- Only business questions answered
- Pre-aggregated and optimized for BI/reporting
- Gold tables answer questions, not store events

## 4. Gold Layer Core Objectives (What You Should Build)

### Objective 1: Revenue & Sales Performance

**Business Question**  
How much revenue is generated, when, and from where?

**Gold Objectives**  
- Daily, monthly, and yearly revenue trends
- Revenue by country
- Revenue by invoice (order-level metrics)

**Key Metrics**  
- Total Revenue
- Number of Orders
- Average Order Value (AOV)
- Revenue per Country
- Revenue per Month

**Gold Tables**  
- `gold.fact_sales_daily`  
- `gold.fact_sales_monthly`  
- `gold.fact_sales_country`

### Objective 2: Customer Analytics

**Business Question**  
Who are the valuable customers and how do they behave?

**Gold Objectives**  
- Identify high-value customers (wholesalers vs. retail)
- Customer purchasing frequency
- Customer lifetime value (basic CLV proxy)

**Key Metrics**  
- Total Spend per Customer
- Number of Invoices
- Average Basket Size
- First Purchase Date
- Last Purchase Date

**Gold Tables**  
- `gold.dim_customer`  
- `gold.fact_customer_value`

**Important Note**  
Customers with NULL `CustomerID` should be excluded or isolated, not mixed.

### Objective 3: Product Performance

**Business Question**  
Which products drive revenue and volume?

**Gold Objectives**  
- Top products by revenue
- Top products by quantity sold
- Identify bulk/wholesale items
- Detect low-margin high-volume items

**Key Metrics**  
- Total Quantity Sold
- Total Revenue per Product
- Average Unit Price
- Revenue Contribution %

**Gold Tables**  
- `gold.dim_product`  
- `gold.fact_product_sales`

### Objective 4: Time-Based & Seasonality Analysis

**Business Question**  
When do customers buy and how does seasonality affect sales?

**Gold Objectives**  
- Month-over-month trends
- Day-of-week behavior
- Hour-of-day patterns

**Key Metrics**  
- Revenue by Month
- Revenue by Weekday
- Revenue by Hour

**Gold Tables**  
- `gold.dim_date`  
- `gold.fact_time_analysis`

### Objective 5: Data Quality & Business Rules Enforcement

**Business Question**  
Can the data be trusted for decision-making?

**Gold Objectives**  
- Exclude cancellations (`InvoiceNo` LIKE 'C%')
- Exclude zero or negative revenue rows
- Separate operational charges (postage, manual, fees)
- Ensure one business meaning per table

**Gold Outcome**  
Executives can query Gold tables without knowing raw data quirks
