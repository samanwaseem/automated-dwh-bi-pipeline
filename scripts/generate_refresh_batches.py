#!/usr/bin/env python3
"""
generate_refresh_batches.py - Oversamples Phase 1 data into two 2026 refresh batches:
  data/refresh1  (Jan 2026: 5,000 sales rows + 1 month of hospital demand)
  data/refresh2  (Feb 2026: 5,000 sales rows + 1 month of hospital demand)
"""
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

BATCHES = [
    {"name": "refresh1", "prefix": "R1", "month": "2026-01", "days": 31, "seed": 101},
    {"name": "refresh2", "prefix": "R2", "month": "2026-02", "days": 28, "seed": 202},
]
SALES_ROWS_PER_BATCH = 5000


def make_batch(master_cols, sales_df, demand_month_df, cfg):
    rng = np.random.default_rng(cfg["seed"])
    out_dir = DATA_DIR / cfg["name"]
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Master data: header only (keeps stg_master unique on append)
    pd.DataFrame(columns=master_cols).to_csv(out_dir / "01_master_data.csv", index=False)

    # 2. Sales data: sample 5,000 rows, assign new IDs & 2026 dates, jitter quantities
    s = sales_df.sample(n=SALES_ROWS_PER_BATCH, replace=True, random_state=cfg["seed"]).copy()
    s["SalesTransactionID"] = [f"TXN-{cfg['prefix']}-{i:06d}" for i in range(1, len(s) + 1)]
    days = rng.integers(1, cfg["days"] + 1, size=len(s))
    s["SalesDate"] = [f"{cfg['month']}-{d:02d}" for d in days]

    # Jitter quantity by ±10% and recalculate financials so validation rules pass
    factor = rng.uniform(0.90, 1.10, size=len(s))
    s["QuantitySoldPacks"] = np.maximum(1, np.round(pd.to_numeric(s["QuantitySoldPacks"]) * factor)).astype(int)
    unit_price = pd.to_numeric(s["UnitRetailPriceUSD"])
    gross = np.round(s["QuantitySoldPacks"] * unit_price, 2)
    disc_rate = np.where(pd.to_numeric(s["GrossSalesAmountUSD"]) > 0,
                         pd.to_numeric(s["DiscountAmountUSD"]) / pd.to_numeric(s["GrossSalesAmountUSD"]), 0.0)
    disc = np.round(gross * disc_rate, 2)
    net = np.round(gross - disc, 2)
    cogs_ratio = np.where(pd.to_numeric(s["NetSalesAmountUSD"]) > 0,
                          pd.to_numeric(s["CostOfGoodsSoldUSD"]) / pd.to_numeric(s["NetSalesAmountUSD"]), 0.65)
    cogs = np.round(net * cogs_ratio, 2)

    s["GrossSalesAmountUSD"] = gross
    s["DiscountAmountUSD"] = disc
    s["NetSalesAmountUSD"] = net
    if "GrossMarginUSD" in s.columns:
        s["GrossMarginUSD"] = np.round(net - cogs, 2)
    s["CostOfGoodsSoldUSD"] = cogs

    s.to_csv(out_dir / "05_pharmacy_sales_data.csv", index=False)

    # 3. Demand data: take 1 full month, shift to 2026, jitter slightly, recalculate closing stock
    d = demand_month_df.copy()
    d["DemandMonth"] = cfg["month"]
    if "DemandRecordID" in d.columns:
        d["DemandRecordID"] = [f"DEM-{cfg['prefix']}-{i:05d}" for i in range(1, len(d) + 1)]

    for col in ["ForecastDemandPacks", "ConsumedQuantityPacks", "DispensedQuantityPacks"]:
        j = rng.uniform(0.95, 1.05, size=len(d))
        d[col] = np.maximum(0, np.round(pd.to_numeric(d[col]) * j)).astype(int)

    if "ClosingStockPacks" in d.columns:
        d["ClosingStockPacks"] = (
            pd.to_numeric(d["OpeningStockPacks"]).astype(int)
            + pd.to_numeric(d["ReceivedQuantityPacks"]).astype(int)
            - pd.to_numeric(d["DispensedQuantityPacks"]).astype(int)
            - pd.to_numeric(d["WastageQuantityPacks"]).astype(int)
        )

    d.to_csv(out_dir / "06_hospital_demand_data.csv", index=False)
    print(f"[{cfg['name']}] -> {out_dir}")
    print(f"   sales: {len(s):,} rows ({cfg['month']}) | demand: {len(d):,} rows ({cfg['month']})")


def main():
    print("Reading source CSVs...")
    master_cols = pd.read_csv(DATA_DIR / "01_master_data.csv", nrows=0).columns.tolist()
    sales_df = pd.read_csv(DATA_DIR / "05_pharmacy_sales_data.csv", dtype=str)
    demand_df = pd.read_csv(DATA_DIR / "06_hospital_demand_data.csv", dtype=str)

    latest_month = demand_df["DemandMonth"].max()
    demand_month_df = demand_df[demand_df["DemandMonth"] == latest_month].copy()

    for cfg in BATCHES:
        make_batch(master_cols, sales_df, demand_month_df, cfg)
    print("\nDone! Ready to trigger Airflow in append mode.")


if __name__ == "__main__":
    main()