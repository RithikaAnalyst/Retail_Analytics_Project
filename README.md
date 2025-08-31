#  Retail Analytics Project

## Overview
This project builds a **Retail Analytics pipeline** combining:
- **SQL (MSSQL)** → Full schema, data load (from CSV), analytics queries.
- **Python (Pandas, scikit-learn, Matplotlib)** → ETL, KPIs, forecasting, RFM segmentation, and visualizations.

It simulates a real-world **retail data warehouse** and **business insights system**.

---

## ⚙️ Tech Stack
- **SQL Server** – Database schema, staging loads, analytics queries.
- **Python 3** – ETL, transformations, KPIs, forecasting.
- **Pandas / NumPy** – Data processing.
- **Scikit-learn** – Linear regression forecast.
- **Matplotlib** – Visualizations.
- **Excel** – Final structured outputs for business users.

---

## 📂 Project Structure
Retail-Analytics-Project/

│

├── sql/

│ └── retail_analytics_build.sql # DB schema, bulk load, queries

├── python/

│ └── etl_and_forecast.py # ETL pipeline + forecasting

├── data/ # Input CSVs (customers, products, stores, transactions, inventory)

├── outputs/ # Generated outputs (ignored in .gitignore)


├── requirements.txt # Python dependencies

└── README.md # Project documentation



---
## Key Analytics & KPIs
### SQL Queries

- Revenue by Month
- Top 10 Products by Revenue
- Customer Segmentation (High / Medium / Low)
- Inventory Stockouts (closing stock < 10)
- Store Performance KPIs

### Python Outputs
- Cleaned Transactions → removes invalid rows, recalculates amounts
- Monthly KPIs → orders, revenue, avg discount, unique customers
- Revenue Forecast → Linear Regression / Naive (last 3 months)
- RFM Segmentation → Recency, Frequency, Monetary scoring


## 🚀 How to Run

### 1️⃣ SQL Setup
1. Open **SQL Server Management Studio (SSMS)**.  
2. Run the script:  
- Creates the database: `Retail_Analytics_Project`  
- Defines tables, staging areas, and relationships  
- Bulk loads CSV data into tables  
- Runs sample analytics queries  
3. Update `BULK INSERT` file paths to match your system.

``` SQL
/* =========================================================
   Retail Analytics Project - FULL BUILD + LOAD + ANALYTICS
   ========================================================= */

-- Create and switch DB
IF DB_ID('Retail_Analytics_Project') IS NULL
BEGIN
    CREATE DATABASE Retail_Analytics_Project;
END
GO
USE Retail_Analytics_Project;
GO

/* =========================
   1) BASE TABLES (IF MISSING)
   ========================= */

-- Customers
IF OBJECT_ID('dbo.customers','U') IS NULL
BEGIN
    CREATE TABLE dbo.customers (
      customer_id INT PRIMARY KEY,
      customer_name VARCHAR(100),
      gender CHAR(1),
      age INT,
      city VARCHAR(100),
      state VARCHAR(100),
      region VARCHAR(50),
      signup_date DATE,
      segment VARCHAR(20)
    );
END

-- Products
IF OBJECT_ID('dbo.products','U') IS NULL
BEGIN
    CREATE TABLE dbo.products (
      product_id INT PRIMARY KEY,
      category VARCHAR(100),
      subcategory VARCHAR(100),
      product_name VARCHAR(150),
      cost DECIMAL(10,2),
      price DECIMAL(10,2)
    );
END

-- Stores
IF OBJECT_ID('dbo.stores','U') IS NULL
BEGIN
    CREATE TABLE dbo.stores (
      store_id INT PRIMARY KEY,
      store_name VARCHAR(100),
      city VARCHAR(100),
      state VARCHAR(100),
      region VARCHAR(50)
    );
END

-- Transactions (child of customers/products/stores)
IF OBJECT_ID('dbo.transactions','U') IS NULL
BEGIN
    CREATE TABLE dbo.transactions (
      transaction_id INT PRIMARY KEY,
      transaction_date DATE NOT NULL,
      customer_id INT NOT NULL,
      store_id INT NOT NULL,
      product_id INT NOT NULL,
      quantity INT,
      discount_pct DECIMAL(5,2),
      payment_method VARCHAR(50),
      gross_amount DECIMAL(12,2)
    );

    ALTER TABLE dbo.transactions
      ADD CONSTRAINT FK_transactions_customers
          FOREIGN KEY (customer_id) REFERENCES dbo.customers(customer_id),
          CONSTRAINT FK_transactions_stores
          FOREIGN KEY (store_id) REFERENCES dbo.stores(store_id),
          CONSTRAINT FK_transactions_products
          FOREIGN KEY (product_id) REFERENCES dbo.products(product_id);
END

-- Inventory (child of products)
IF OBJECT_ID('dbo.inventory','U') IS NULL
BEGIN
    CREATE TABLE dbo.inventory (
      product_id INT NOT NULL,
      [date] DATE NOT NULL,
      opening_stock INT,
      purchased INT,
      sold INT,
      closing_stock INT,
      CONSTRAINT PK_inventory PRIMARY KEY (product_id, [date]),
      CONSTRAINT FK_inventory_products FOREIGN KEY (product_id) REFERENCES dbo.products(product_id)
    );
END
GO

/* =========================================
   2) MASTER DATA LOADS (STAGING + MERGE)
   ========================================= */

-- ========== CUSTOMERS ==========
IF OBJECT_ID('dbo.customers_staging','U') IS NOT NULL DROP TABLE dbo.customers_staging;
CREATE TABLE dbo.customers_staging (
  customer_id INT,
  customer_name VARCHAR(100),
  gender CHAR(1),
  age INT,
  city VARCHAR(100),
  state VARCHAR(100),
  region VARCHAR(50),
  signup_date DATE,
  segment VARCHAR(20)
);

BULK INSERT dbo.customers_staging
FROM 'C:\Users\rithi\OneDrive\Documents\fielsss\Airbin\Sales analysis\customers.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);

MERGE dbo.customers AS target
USING dbo.customers_staging AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET customer_name = source.customer_name,
               gender = source.gender,
               age = source.age,
               city = source.city,
               state = source.state,
               region = source.region,
               signup_date = source.signup_date,
               segment = source.segment
WHEN NOT MATCHED BY TARGET THEN
    INSERT (customer_id, customer_name, gender, age, city, state, region, signup_date, segment)
    VALUES (source.customer_id, source.customer_name, source.gender, source.age,
            source.city, source.state, source.region, source.signup_date, source.segment);

DROP TABLE dbo.customers_staging;
GO

-- ========== PRODUCTS ==========
IF OBJECT_ID('dbo.products_staging','U') IS NOT NULL DROP TABLE dbo.products_staging;
CREATE TABLE dbo.products_staging (
  product_id INT,
  category VARCHAR(100),
  subcategory VARCHAR(100),
  product_name VARCHAR(150),
  cost DECIMAL(10,2),
  price DECIMAL(10,2)
);

BULK INSERT dbo.products_staging
FROM 'C:\Users\rithi\OneDrive\Documents\fielsss\Airbin\Sales analysis\products.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);

MERGE dbo.products AS target
USING dbo.products_staging AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
    UPDATE SET category = source.category,
               subcategory = source.subcategory,
               product_name = source.product_name,
               cost = source.cost,
               price = source.price
WHEN NOT MATCHED BY TARGET THEN
    INSERT (product_id, category, subcategory, product_name, cost, price)
    VALUES (source.product_id, source.category, source.subcategory,
            source.product_name, source.cost, source.price);

DROP TABLE dbo.products_staging;
GO

-- ========== STORES ==========
IF OBJECT_ID('dbo.stores_staging','U') IS NOT NULL DROP TABLE dbo.stores_staging;
CREATE TABLE dbo.stores_staging (
  store_id INT,
  store_name VARCHAR(100),
  city VARCHAR(100),
  state VARCHAR(100),
  region VARCHAR(50)
);

BULK INSERT dbo.stores_staging
FROM 'C:\Users\rithi\OneDrive\Documents\fielsss\Airbin\Sales analysis\stores.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);

MERGE dbo.stores AS target
USING dbo.stores_staging AS source
ON target.store_id = source.store_id
WHEN MATCHED THEN
    UPDATE SET store_name = source.store_name,
               city = source.city,
               state = source.state,
               region = source.region
WHEN NOT MATCHED BY TARGET THEN
    INSERT (store_id, store_name, city, state, region)
    VALUES (source.store_id, source.store_name, source.city, source.state, source.region);

DROP TABLE dbo.stores_staging;
GO

/* ==================================
   3) FACT/DAILY TABLES RELOAD (SAFE)
   ================================== */

-- Transactions: child table → TRUNCATE is allowed (no children reference it)
TRUNCATE TABLE dbo.transactions;

BULK INSERT dbo.transactions
FROM 'C:\Users\rithi\OneDrive\Documents\fielsss\Airbin\Sales analysis\transactions.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);

-- Inventory: child of products → TRUNCATE is allowed (no children reference it)
TRUNCATE TABLE dbo.inventory;

BULK INSERT dbo.inventory
FROM 'C:\Users\rithi\OneDrive\Documents\fielsss\Airbin\Sales analysis\inventory.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
GO

/* ==========================
   4) ANALYTICS QUERIES
   ========================== */
USE  Retail_Analytics_Project;
-- 1) Total Revenue by Month (works on all SQL Server versions)
SELECT 
    CAST(DATEFROMPARTS(YEAR(transaction_date), MONTH(transaction_date), 1) AS DATE) AS [month],
    SUM(gross_amount) AS revenue
FROM dbo.transactions
GROUP BY DATEFROMPARTS(YEAR(transaction_date), MONTH(transaction_date), 1)
ORDER BY [month];

-- 2) Top 10 Products by Revenue
SELECT TOP 10
    p.product_id,
    p.product_name,
    p.category,
    SUM(t.gross_amount) AS revenue
FROM dbo.transactions t
JOIN dbo.products p 
    ON p.product_id = t.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY revenue DESC;

-- 3) Customer Segmentation: High / Medium / Low based on total spend
WITH spend AS (
    SELECT 
        c.customer_id, 
        c.customer_name, 
        SUM(t.gross_amount) AS total_spend
    FROM dbo.customers c
    JOIN dbo.transactions t 
        ON t.customer_id = c.customer_id
    GROUP BY c.customer_id, c.customer_name
),
bounds AS (
    SELECT 
        PERCENTILE_CONT(0.33) WITHIN GROUP (ORDER BY total_spend) OVER () AS p33,
        PERCENTILE_CONT(0.66) WITHIN GROUP (ORDER BY total_spend) OVER () AS p66
    FROM spend
)
SELECT 
    s.customer_id, 
    s.customer_name, 
    s.total_spend,
    CASE
        WHEN s.total_spend >= b.p66 THEN 'High'
        WHEN s.total_spend >= b.p33 THEN 'Medium'
        ELSE 'Low'
    END AS segment
FROM spend s
CROSS JOIN (SELECT DISTINCT p33, p66 FROM bounds) b
ORDER BY s.total_spend DESC;

-- 4) Inventory Stockouts (closing stock below 10)
SELECT 
    i.[date], 
    i.product_id, 
    p.product_name, 
    i.closing_stock
FROM dbo.inventory i
JOIN dbo.products p 
    ON p.product_id = i.product_id
WHERE i.closing_stock < 10
ORDER BY i.[date] DESC, i.closing_stock ASC;

-- 5) Store Performance KPIs
SELECT 
    s.store_id, 
    s.store_name, 
    s.city, 
    s.region,
    COUNT(DISTINCT t.transaction_id) AS orders,
    SUM(t.gross_amount) AS revenue,
    AVG(t.discount_pct) AS avg_discount
FROM dbo.transactions t
JOIN dbo.stores s 
    ON s.store_id = t.store_id
GROUP BY s.store_id, s.store_name, s.city, s.region
ORDER BY revenue DESC;

```

### 2️⃣ Python ETL & Forecast
1. Install dependencies:
```bash
pip install -r requirements.txt
```
``` Python code
"""
Retail Analytics Project - ETL & Forecast

Usage:
    python etl_and_forecast.py

Outputs:
    - outputs/clean_transactions.parquet
    - outputs/kpis_monthly.csv
    - outputs/forecast_next_month.csv
    - outputs/rfm_scores.csv
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

try:
    from sklearn.linear_model import LinearRegression
except Exception:
    LinearRegression = None



BASE = Path(__file__).resolve().parents[1]
DATA = BASE / "data"
OUT = BASE / "outputs"
OUT.mkdir(exist_ok=True)

def load_data():
    transactions = pd.read_csv(DATA / "transactions.csv", parse_dates=["transaction_date"])
    customers = pd.read_csv(DATA / "customers.csv", parse_dates=["signup_date"])
    products = pd.read_csv(DATA / "products.csv")
    stores = pd.read_csv(DATA / "stores.csv")
    return transactions, customers, products, stores

def clean_transactions(transactions, products):
    df = transactions.copy()
    df = df[df["quantity"] > 0]
    df = df[(df["discount_pct"] >= 0) & (df["discount_pct"] <= 0.9)]
    price_map = products.set_index("product_id")["price"]
    df["unit_price"] = df["product_id"].map(price_map)
    df["computed_amount"] = (df["unit_price"] * df["quantity"] * (1 - df["discount_pct"])).round(2)
    df["amount"] = df["computed_amount"]
    return df

def kpis_monthly(df):
    df["month"] = df["transaction_date"].values.astype("datetime64[M]")
    kpis = df.groupby("month").agg(
        orders=("transaction_id","nunique"),
        revenue=("amount","sum"),
        avg_discount=("discount_pct","mean"),
        customers=("customer_id","nunique")
    ).reset_index()
    return kpis

def simple_forecast_linear(kpis):
    if LinearRegression is None or len(kpis) < 6:
        fc_value = float(kpis["revenue"].tail(3).mean())
        return pd.DataFrame([{
            "forecast_month": (pd.to_datetime(kpis["month"].max()) + pd.offsets.MonthBegin(1)).strftime("%Y-%m"),
            "method": "naive_mean_last3",
            "forecast_revenue": round(fc_value, 2)
        }])
    kpis = kpis.copy()
    kpis["t"] = np.arange(len(kpis))
    X = kpis[["t"]].values
    y = kpis["revenue"].values
    model = LinearRegression().fit(X, y)
    next_t = np.array([[kpis["t"].max() + 1]])
    fc_value = float(model.predict(next_t)[0])
    return pd.DataFrame([{
        "forecast_month": (pd.to_datetime(kpis["month"].max()) + pd.offsets.MonthBegin(1)).strftime("%Y-%m"),
        "method": "linear_regression",
        "forecast_revenue": round(fc_value, 2)
    }])

def main():
    transactions, customers, products, stores = load_data()
    tx = clean_transactions(transactions, products)
    tx.to_parquet(OUT / "clean_transactions.parquet", index=False)

    kpis = kpis_monthly(tx)
    kpis.to_csv(OUT / "kpis_monthly.csv", index=False)

    fc = simple_forecast_linear(kpis)
    fc.to_csv(OUT / "forecast_next_month.csv", index=False)

    latest_date = tx["transaction_date"].max()
    rfm = tx.groupby("customer_id").agg(
        recency=("transaction_date", lambda x: (latest_date - x.max()).days),
        frequency=("transaction_id","nunique"),
        monetary=("amount","sum")
    )
    r_labels = [5,4,3,2,1]
    f_labels = m_labels = [1,2,3,4,5]
    r_quintiles = pd.qcut(rfm["recency"], 5, labels=r_labels)
    f_quintiles = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=f_labels)
    m_quintiles = pd.qcut(rfm["monetary"], 5, labels=m_labels)
    rfm["R"] = r_quintiles.astype(int)
    rfm["F"] = f_quintiles.astype(int)
    rfm["M"] = m_quintiles.astype(int)
    rfm["RFM_Score"] = rfm["R"]*100 + rfm["F"]*10 + rfm["M"]
    rfm_reset = rfm.reset_index()
    rfm_reset.to_csv(OUT / "rfm_scores.csv", index=False)
    print("Writing Excel file...")

    with pd.ExcelWriter(OUT / "retail_analysis_outputs.xlsx", engine="openpyxl") as writer:
        tx.to_excel(writer, sheet_name="Clean_Transactions", index=False)
        kpis.to_excel(writer, sheet_name="KPIs_Monthly", index=False)
        fc.to_excel(writer, sheet_name="Forecast", index=False)
        rfm_reset.to_excel(writer, sheet_name="RFM_Scores", index=False)

    print("Excel file created successfully!")

    # ==========================
    # VISUALS SECTION (INSIDE MAIN)
    # ==========================
    import matplotlib.pyplot as plt

    # 1. Revenue Trend + Forecast
    plt.figure(figsize=(8,5))
    plt.plot(kpis["month"], kpis["revenue"], marker="o", label="Historical Revenue")
    plt.axvline(kpis["month"].max(), color="gray", linestyle="--")
    plt.scatter(pd.to_datetime(fc["forecast_month"]), fc["forecast_revenue"],
                color="red", label="Forecast", zorder=5)
    plt.title("Monthly Revenue with Forecast")
    plt.xlabel("Month")
    plt.ylabel("Revenue")
    plt.legend()
    plt.tight_layout()
    plt.show()

    # 2. Top 5 Customers by Monetary
    top5 = rfm_reset.nlargest(5, "monetary")
    plt.figure(figsize=(7,5))
    plt.bar(top5["customer_id"].astype(str), top5["monetary"], color="skyblue")
    plt.title("Top 5 Customers by Monetary Value")
    plt.xlabel("Customer ID")
    plt.ylabel("Monetary Value")
    plt.tight_layout()
    plt.show()

    # 3. RFM Scatter Plot
    plt.figure(figsize=(7,5))
    plt.scatter(rfm_reset["recency"], rfm_reset["frequency"],
                s=rfm_reset["monetary"]/50, alpha=0.6,
                c=rfm_reset["RFM_Score"], cmap="viridis")
    plt.colorbar(label="RFM Score")
    plt.title("Customer Segmentation (RFM)")
    plt.xlabel("Recency (days since last purchase)")
    plt.ylabel("Frequency (# transactions)")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
```
## Visualizations (auto-shown):

📈![Monthly Revenue with Forecast](https://github.com/RithikaAnalyst/Retail_Analytics_Project/blob/main/Monthly%20Revenue%20with%20Forecast.png)

👥![Top 5 Customers by MonetaryValue](https://github.com/RithikaAnalyst/Retail_Analytics_Project/blob/main/Top%205%20Customers%20by%20Monetary%20Value.png)

🔎 ![RFM Segmentation Scatter Plot](https://github.com/RithikaAnalyst/Retail_Analytics_Project/blob/main/RFM%20Segmentation%20Scatter%20Plot.png)

## Future Enhancements

- Add advanced ML forecasting (Prophet, XGBoost)
- Build Power BI dashboards on SQL DB
- Automate ETL with Airflow or Prefect
- Deploy on Azure SQL + Azure Data Factory

## Author

Developed by [Rithika R] 

📧 Contact: [rithikaramalingam37@gmail.com]

💼 Aspiring Data Analyst | Data Engineer
