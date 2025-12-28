## Silver Layer (Cleaned & Conformed Data)

**Description**  
The Silver layer represents the **cleaned, standardized, and conformed** version of the raw transactional data.  
At this stage, the data is transformed from a single wide transactional table into a **star-schema–ready analytical model**, while still preserving transaction-level detail.

The Silver layer serves as the **single source of truth** for downstream analytical modeling in the Gold layer.

---

### Key Responsibilities of the Silver Layer

- Enforce core **data quality rules**
- Standardize data types and formats
- Deduplicate transactional records
- Conform shared dimensions across the dataset
- Introduce **surrogate keys** for dimensional modeling
- Prepare data for fast analytical joins and aggregations

---

### Key Transformations Applied

- Remove duplicate transaction rows
- Exclude cancelled invoices (`InvoiceNo LIKE 'C%'`)
- Enforce valid revenue logic (`Quantity > 0` and `UnitPrice > 0`)
- Standardize date and time formats
- Preserve `NULL` `CustomerID` values to support guest checkouts
- Resolve natural keys into surrogate keys during fact loading

---

### Silver Layer Data Model

The Silver layer is implemented using a **Star Schema**, consisting of one central fact table and multiple conformed dimensions.

#### Fact Tables

- **`silver.fact_main`**  
  - Deduplicated transactional source table  
  - One row per raw invoice line  
  - Acts as the canonical cleaned dataset in Silver

- **`silver.fact_sales`**  
  - Analytical fact table  
  - Grain: **one row per invoice line (Product × Invoice × Date)**  
  - Stores measurable metrics such as quantity, unit price, and line-level revenue  
  - Uses surrogate keys to reference all dimensions

---

#### Dimension Tables

- **`silver.dim_product`**  
  - Product master data  
  - Attributes include stock code, product name, and first-seen date

- **`silver.dim_customer`**  
  - Customer master data  
  - Tracks customer purchase lifecycle (first and last purchase dates)  
  - Supports nullable customers (guest transactions)

- **`silver.dim_country`**  
  - Conformed geographic dimension  
  - One row per country with enforced uniqueness

- **`silver.dim_date`**  
  - Calendar dimension  
  - One row per calendar day  
  - Enables time-based and seasonality analysis

---

### Design Notes

- The Silver layer maintains **transaction-level granularity**
- No business aggregations are performed at this stage
- All tables are designed to be **re-buildable and deterministic**
- Business aggregations and KPIs are deferred to the Gold layer

---

### Outcome

The Silver layer delivers a **clean, reliable, and analytically structured dataset** that can be consumed directly by the Gold layer for business-focused reporting, dashboards, and advanced analytics.
