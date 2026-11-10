"""Step 3: build the DuckDB star schema (dimensions, then two fact tables) from the Postgres staging tables.
Run from the IDA PROJECT folder after load_staging.py and validate_staging.py:   python build_star_schema.py

Design in one line: two facts (fact_sales = one row per sale, fact_demand = one row per hospital + medicine + month)
sharing dim_medicine; each fact also has its own dimensions. Pharmacies can exist without a hospital (hospital_key = 0).

Rerunnable (idempotent): every table is rebuilt with CREATE OR REPLACE, so running it twice never duplicates rows.
Rows rejected by validate_staging.py are left out, and rows that cannot find a dimension row go to dq_orphan_rows.

WHY (write these in your own words before submitting; the professor marks your reasoning):
  - why two fact tables:
  - why GTIN14 is the medicine key:
  - why a junk dimension for the sales flags:
  - why hospital_key lives in dim_pharmacy (0 = no hospital):
  - why DuckDB for the warehouse:
"""
import duckdb
import pandas as pd
from sqlalchemy import create_engine

PG_URL = "postgresql+psycopg2://dwh_user:dwh_pass@localhost:5433/staging"
DUCK_FILE = "warehouse.duckdb"

M_COLS = ["MedicineID", "InternalSKU", "GTIN14", "GenericName", "ICD10Category", "DiseaseCategory", "TherapeuticClass",
          "DosageForm", "Strength", "RouteOfAdministration", "PackSize", "BaseUnitOfMeasure", "PrescriptionClass",
          "ColdChainFlag", "ShelfLifeMonths", "UnitManufacturingCostUSD", "WholesalePriceUSD",
          "RecommendedRetailPriceUSD", "ProductStatus", "EffectiveFromDate"]
S_COLS = ["SalesTransactionID", "SalesDate", "SalesChannel", "CustomerSegment", "DeliveryOTIFFlag", "PharmacyID",
          "PharmacyGLN", "PharmacyName", "PharmacyType", "PharmacyCity", "PharmacyState", "HospitalID", "GTIN14",
          "DaysToExpiryAtSale", "QuantitySoldPacks", "UnitRetailPriceUSD", "GrossSalesAmountUSD", "DiscountAmountUSD",
          "NetSalesAmountUSD", "SalesTaxUSD", "CostOfGoodsSoldUSD", "GrossMarginUSD"]
D_COLS = ["HospitalDemandRecordID", "DemandMonth", "HospitalID", "HospitalGLN", "HospitalName", "HospitalType",
          "LicensedBeds", "ICUBeds", "AverageOccupancyRatePct", "City", "State", "GTIN14", "EstimatedDiseaseCases",
          "DiseaseRelatedAdmissions", "OutpatientEncounters", "ForecastDemandPacks", "RequestedQuantityPacks",
          "ApprovedQuantityPacks", "ReceivedQuantityPacks", "EmergencyRequestQuantityPacks", "OpeningStockPacks",
          "DispensedQuantityPacks", "ConsumedQuantityPacks", "WastageQuantityPacks", "ClosingStockPacks",
          "DemandTrendIndex"]


def load_clean(con, eng, table, cols, id_col):
    """Copy a staging table into DuckDB (temporary), leaving out the rows validation rejected."""
    q = ", ".join(f'"{c}"' for c in cols)
    df = pd.read_sql(f"SELECT {q} FROM {table}", eng)
    con.register(f"{table}_raw", df)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {table}_ok AS
        SELECT * FROM {table}_raw
        WHERE {id_col} NOT IN (SELECT record_id FROM rejected WHERE table_name = '{table}')""")
    n_all, n_ok = len(df), con.execute(f"SELECT COUNT(*) FROM {table}_ok").fetchall()[0][0]
    print(f"  {table}: {n_all} staging rows, {n_all - n_ok} rejected by validation, {n_ok} used")
    return n_ok


def main():
    eng = create_engine(PG_URL)
    con = duckdb.connect(DUCK_FILE)
    con.execute("""CREATE OR REPLACE TEMP MACRO parse_date(x) AS
        COALESCE(TRY_STRPTIME(x, '%m/%d/%Y'), TRY_STRPTIME(x, '%Y-%m-%d'))::DATE""")  # source files use either format
    con.register("rejected_raw", pd.read_sql("SELECT table_name, record_id FROM rejected_rows", eng))
    con.execute("CREATE OR REPLACE TEMP TABLE rejected AS SELECT * FROM rejected_raw")

    print("Reading staging:")
    n_master = load_clean(con, eng, "stg_master", M_COLS, "MedicineID")
    n_sales = load_clean(con, eng, "stg_sales", S_COLS, "SalesTransactionID")
    n_demand = load_clean(con, eng, "stg_demand", D_COLS, "HospitalDemandRecordID")

    # ---------------- dimensions ----------------
    con.execute("""CREATE OR REPLACE TABLE dim_date AS
        SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key, d AS full_date, YEAR(d) AS year, QUARTER(d) AS quarter,
               MONTH(d) AS month, strftime(d, '%B') AS month_name, strftime(d, '%Y-%m') AS year_month,
               DAY(d) AS day_of_month, strftime(d, '%A') AS day_name, ISODOW(d) IN (6, 7) AS is_weekend
        FROM (SELECT CAST(range AS DATE) AS d FROM range(DATE '2023-01-01', DATE '2026-01-01', INTERVAL 1 DAY))""")

    # natural key = GTIN14 (the identifier present in all three files). Type 2 columns: valid_from / valid_to / is_current.
    con.execute("""CREATE OR REPLACE TABLE dim_medicine AS
        SELECT ROW_NUMBER() OVER (ORDER BY GTIN14) AS medicine_key,
               GTIN14 AS gtin14, MedicineID AS medicine_id, InternalSKU AS internal_sku, GenericName AS generic_name,
               TherapeuticClass AS therapeutic_class, DiseaseCategory AS disease_category, ICD10Category AS icd10_category,
               DosageForm || ' ' || Strength AS dosage_form_strength, RouteOfAdministration AS route_of_administration,
               TRY_CAST(PackSize AS INTEGER) AS pack_size,
               CASE WHEN TRY_CAST(PackSize AS DOUBLE) > 15 THEN 'Large' ELSE 'Small' END AS pack_size_group,
               BaseUnitOfMeasure AS base_unit_of_measure, PrescriptionClass AS prescription_class,
               ColdChainFlag AS cold_chain_flag, TRY_CAST(ShelfLifeMonths AS INTEGER) AS shelf_life_months,
               CASE WHEN TRY_CAST(ShelfLifeMonths AS INTEGER) = 36 THEN 'L' ELSE 'SL' END AS shelf_life_group,
               ProductStatus AS product_status,
               TRY_CAST(UnitManufacturingCostUSD AS DOUBLE) AS standard_cost_usd,
               TRY_CAST(WholesalePriceUSD AS DOUBLE) AS list_trade_price_usd,
               TRY_CAST(RecommendedRetailPriceUSD AS DOUBLE) AS mrp_usd,
               parse_date(EffectiveFromDate) AS valid_from, DATE '9999-12-31' AS valid_to, TRUE AS is_current
        FROM stg_master_ok""")

    con.execute("""CREATE OR REPLACE TABLE dim_location AS
        SELECT ROW_NUMBER() OVER (ORDER BY state, city) AS location_key, state, city
        FROM (SELECT DISTINCT PharmacyState AS state, PharmacyCity AS city FROM stg_sales_ok
              UNION SELECT DISTINCT State, City FROM stg_demand_ok)""")

    # hospital_key 0 = "No hospital": pharmacies can exist without one
    con.execute("""CREATE OR REPLACE TABLE dim_hospital AS
        SELECT 0 AS hospital_key, 'N/A' AS hospital_id, CAST(NULL AS VARCHAR) AS hospital_gln, 'No hospital' AS hospital_name,
               CAST(NULL AS VARCHAR) AS hospital_type, CAST(NULL AS INTEGER) AS licensed_beds, CAST(NULL AS INTEGER) AS icu_beds,
               CAST(NULL AS DOUBLE) AS occupancy_pct, CAST(NULL AS INTEGER) AS location_key
        UNION ALL
        SELECT ROW_NUMBER() OVER (ORDER BY h.HospitalID), h.HospitalID, h.HospitalGLN, h.HospitalName, h.HospitalType,
               TRY_CAST(h.LicensedBeds AS INTEGER), TRY_CAST(h.ICUBeds AS INTEGER),
               TRY_CAST(h.AverageOccupancyRatePct AS DOUBLE), l.location_key
        FROM (SELECT DISTINCT HospitalID, HospitalGLN, HospitalName, HospitalType, LicensedBeds, ICUBeds,
                     AverageOccupancyRatePct, City, State FROM stg_demand_ok) h
        JOIN dim_location l ON l.state = h.State AND l.city = h.City""")

    con.execute("""CREATE OR REPLACE TABLE dim_pharmacy AS
        SELECT ROW_NUMBER() OVER (ORDER BY p.PharmacyID) AS pharmacy_key, p.PharmacyID AS pharmacy_id,
               p.PharmacyGLN AS pharmacy_gln, p.PharmacyName AS pharmacy_name, p.PharmacyType AS pharmacy_type,
               COALESCE(h.hospital_key, 0) AS hospital_key, l.location_key
        FROM (SELECT DISTINCT PharmacyID, PharmacyGLN, PharmacyName, PharmacyType, PharmacyCity, PharmacyState,
                     NULLIF(HospitalID, '') AS HospitalID FROM stg_sales_ok) p
        JOIN dim_location l ON l.state = p.PharmacyState AND l.city = p.PharmacyCity
        LEFT JOIN dim_hospital h ON h.hospital_id = p.HospitalID""")

    # junk dimension: low-cardinality flags that describe the sale, not the pharmacy
    con.execute("""CREATE OR REPLACE TABLE dim_sales_profile AS
        SELECT ROW_NUMBER() OVER (ORDER BY channel_group, sales_channel, customer_segment, delivery_otif_flag) AS sales_profile_key, *
        FROM (SELECT DISTINCT
                CASE WHEN PharmacyType = 'Hospital Pharmacy' THEN 'Hospital'
                     WHEN SalesChannel = 'Ecommerce' THEN 'E-commerce' ELSE 'Retail' END AS channel_group,
                SalesChannel AS sales_channel, CustomerSegment AS customer_segment, DeliveryOTIFFlag AS delivery_otif_flag
              FROM stg_sales_ok)""")

    # ---------------- rows that cannot find a dimension row ----------------
    con.execute("""CREATE OR REPLACE TABLE dq_orphan_rows AS
        SELECT 'fact_sales' AS target, s.SalesTransactionID AS record_id,
               CASE WHEN m.medicine_key IS NULL THEN 'GTIN14 not in dim_medicine'
                    WHEN p.pharmacy_key IS NULL THEN 'pharmacy not in dim_pharmacy'
                    ELSE 'date not in dim_date' END AS reason
        FROM stg_sales_ok s
        LEFT JOIN dim_medicine m ON m.gtin14 = s.GTIN14
        LEFT JOIN dim_pharmacy p ON p.pharmacy_id = s.PharmacyID
        LEFT JOIN dim_date d ON d.full_date = parse_date(s.SalesDate)
        WHERE m.medicine_key IS NULL OR p.pharmacy_key IS NULL OR d.date_key IS NULL
        UNION ALL
        SELECT 'fact_demand', x.HospitalDemandRecordID,
               CASE WHEN m.medicine_key IS NULL THEN 'GTIN14 not in dim_medicine'
                    WHEN h.hospital_key IS NULL THEN 'hospital not in dim_hospital'
                    ELSE 'date not in dim_date' END
        FROM stg_demand_ok x
        LEFT JOIN dim_medicine m ON m.gtin14 = x.GTIN14
        LEFT JOIN dim_hospital h ON h.hospital_id = x.HospitalID
        LEFT JOIN dim_date d ON d.full_date = parse_date(x.DemandMonth)
        WHERE m.medicine_key IS NULL OR h.hospital_key IS NULL OR d.date_key IS NULL""")

    # ---------------- facts ----------------
    # grain: one row per sales transaction
    con.execute("""CREATE OR REPLACE TABLE fact_sales AS
        SELECT s.SalesTransactionID AS sales_transaction_id,
               d.date_key, m.medicine_key, p.pharmacy_key, j.sales_profile_key,
               TRY_CAST(s.QuantitySoldPacks AS INTEGER) AS quantity_sold_packs,
               TRY_CAST(s.UnitRetailPriceUSD AS DOUBLE) AS unit_retail_price_usd,
               TRY_CAST(s.GrossSalesAmountUSD AS DOUBLE) AS gross_sales_amount_usd,
               TRY_CAST(s.DiscountAmountUSD AS DOUBLE) AS discount_amount_usd,
               TRY_CAST(s.NetSalesAmountUSD AS DOUBLE) AS net_sales_amount_usd,
               TRY_CAST(s.SalesTaxUSD AS DOUBLE) AS sales_tax_usd,
               TRY_CAST(s.CostOfGoodsSoldUSD AS DOUBLE) AS cost_of_goods_sold_usd,
               TRY_CAST(s.GrossMarginUSD AS DOUBLE) AS gross_margin_usd,
               TRY_CAST(s.DaysToExpiryAtSale AS INTEGER) AS days_to_expiry_at_sale
        FROM stg_sales_ok s
        JOIN dim_medicine m ON m.gtin14 = s.GTIN14
        JOIN dim_pharmacy p ON p.pharmacy_id = s.PharmacyID
        JOIN dim_date d ON d.full_date = parse_date(s.SalesDate)
        JOIN dim_sales_profile j ON j.sales_channel = s.SalesChannel AND j.customer_segment = s.CustomerSegment
             AND j.delivery_otif_flag = s.DeliveryOTIFFlag
             AND j.channel_group = CASE WHEN p.pharmacy_type = 'Hospital Pharmacy' THEN 'Hospital'
                                        WHEN s.SalesChannel = 'Ecommerce' THEN 'E-commerce' ELSE 'Retail' END""")

    # grain: one row per hospital + medicine + month (month stored as its first day)
    con.execute("""CREATE OR REPLACE TABLE fact_demand AS
        SELECT d.date_key, m.medicine_key, h.hospital_key,
               TRY_CAST(x.EstimatedDiseaseCases AS DOUBLE) AS estimated_disease_cases,
               TRY_CAST(x.DiseaseRelatedAdmissions AS DOUBLE) AS disease_related_admissions,
               TRY_CAST(x.OutpatientEncounters AS DOUBLE) AS outpatient_encounters,
               TRY_CAST(x.ForecastDemandPacks AS DOUBLE) AS forecast_demand_packs,
               TRY_CAST(x.RequestedQuantityPacks AS DOUBLE) AS requested_quantity_packs,
               TRY_CAST(x.ApprovedQuantityPacks AS DOUBLE) AS approved_quantity_packs,
               TRY_CAST(x.ReceivedQuantityPacks AS DOUBLE) AS received_quantity_packs,
               TRY_CAST(x.EmergencyRequestQuantityPacks AS DOUBLE) AS emergency_request_quantity_packs,
               TRY_CAST(x.OpeningStockPacks AS DOUBLE) AS opening_stock_packs,
               TRY_CAST(x.DispensedQuantityPacks AS DOUBLE) AS dispensed_quantity_packs,
               TRY_CAST(x.ConsumedQuantityPacks AS DOUBLE) AS consumed_quantity_packs,
               TRY_CAST(x.WastageQuantityPacks AS DOUBLE) AS wastage_quantity_packs,
               TRY_CAST(x.ClosingStockPacks AS DOUBLE) AS closing_stock_packs,
               TRY_CAST(x.DemandTrendIndex AS DOUBLE) AS demand_trend_index
        FROM stg_demand_ok x
        JOIN dim_medicine m ON m.gtin14 = x.GTIN14
        JOIN dim_hospital h ON h.hospital_id = x.HospitalID
        JOIN dim_date d ON d.full_date = parse_date(x.DemandMonth)""")

    # ---------------- reconciliation ----------------
    q = lambda sql: con.execute(sql).fetchall()[0][0]
    print("\nStar schema row counts:")
    for t in ["dim_date", "dim_medicine", "dim_location", "dim_hospital", "dim_pharmacy", "dim_sales_profile",
              "fact_sales", "fact_demand", "dq_orphan_rows"]:
        print(f"  {t}: {q(f'SELECT COUNT(*) FROM {t}')}")
    ok = True
    for fact, n_in, tgt in [("fact_sales", n_sales, "fact_sales"), ("fact_demand", n_demand, "fact_demand")]:
        orphans = q(f"SELECT COUNT(*) FROM dq_orphan_rows WHERE target = '{tgt}'")
        rows = q(f"SELECT COUNT(*) FROM {fact}")
        match = rows + orphans == n_in
        ok &= match
        print(f"Check {fact}: {rows} loaded + {orphans} orphans = {rows + orphans} vs {n_in} clean staging rows -> {'OK' if match else 'MISMATCH'}")
    dup_s = q("SELECT COUNT(*) - COUNT(DISTINCT sales_transaction_id) FROM fact_sales")
    dup_d = q("SELECT COUNT(*) - COUNT(DISTINCT (hospital_key, medicine_key, date_key)) FROM fact_demand")
    print(f"Check grain: duplicate sales rows = {dup_s}, duplicate demand rows = {dup_d}")
    print("\nALL CHECKS PASSED" if ok and dup_s == 0 and dup_d == 0 else "\nSOMETHING NEEDS ATTENTION")
    con.close()


if __name__ == "__main__":
    main()