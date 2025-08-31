
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



