#!/usr/bin/env python3
"""
export_to_postgres.py  -  Copy DuckDB star schema tables into PostgreSQL
so that Superset (or any BI tool) can query them directly.

Runs after build_star_schema.py. Creates a 'star' schema in the staging
PostgreSQL database and replaces all 8 tables on every run.
"""

import os
import sys
import logging
import duckdb
import psycopg2
from psycopg2.extras import execute_values

log = logging.getLogger("export_pg")

PG = dict(
    host=os.getenv("PGHOST", "staging-db"),
    port=int(os.getenv("PGPORT", "5432")),
    user=os.getenv("PGUSER", "dwh_user"),
    password=os.getenv("PGPASSWORD", "dwh_pass"),
    dbname=os.getenv("PGDATABASE", "staging"),
)
WAREHOUSE = os.getenv("WAREHOUSE_DB", "warehouse.duckdb")

TABLES = [
    "DimDate", "DimHospital", "DimMedicine", "DimPharmacy",
    "DimLocation", "DimSalesProfile", "FactSales", "FactDemand",
]

# PostgreSQL DDL for the star schema (matches DuckDB exactly)
PG_DDL = {
    "DimDate": """CREATE TABLE IF NOT EXISTS star."DimDate" (
        "DateKey" INTEGER PRIMARY KEY, "FullDate" DATE NOT NULL, "Year" INTEGER, "Quarter" INTEGER,
        "Month" INTEGER, "MonthName" VARCHAR(20), "YearMonth" VARCHAR(10), "DayOfMonth" INTEGER,
        "DayName" VARCHAR(20), "IsWeekend" BOOLEAN, "Season" VARCHAR(20))""",

    "DimHospital": """CREATE TABLE IF NOT EXISTS star."DimHospital" (
        "HospitalKey" INTEGER PRIMARY KEY, "HospitalID" VARCHAR(30) NOT NULL, "HospitalGLN" VARCHAR(20),
        "HospitalName" VARCHAR(150), "HospitalType" VARCHAR(50), "LicensedBeds" INTEGER,
        "ICUBeds" INTEGER, "OccupancyPct" DECIMAL(5,2))""",

    "DimMedicine": """CREATE TABLE IF NOT EXISTS star."DimMedicine" (
        "MedKey" INTEGER PRIMARY KEY, "GTIN14" VARCHAR(20) NOT NULL, "InternalSKU" VARCHAR(30) NOT NULL,
        "MedName" VARCHAR(150), "TherapeuticClass" VARCHAR(100), "ICD10Category" VARCHAR(20),
        "DiseaseName" VARCHAR(100), "DosageFormStrength" VARCHAR(80), "Route" VARCHAR(40),
        "PackSize" INTEGER, "PackSizeGroup" VARCHAR(10), "BaseUnitOfMeasure" VARCHAR(20),
        "PrescClass" VARCHAR(30), "ColdChainFlag" VARCHAR(20), "ShelfLifeMonths" INTEGER,
        "ShelfLifeGroup" VARCHAR(10), "ProductStatus" VARCHAR(20), "StandardCost" DECIMAL(10,2),
        "ListTradePrice" DECIMAL(10,2), "MRP" DECIMAL(10,2), "ValidFrom" DATE NOT NULL,
        "ValidTo" DATE, "IsCurrent" BOOLEAN NOT NULL)""",

    "DimPharmacy": """CREATE TABLE IF NOT EXISTS star."DimPharmacy" (
        "PharmacyKey" INTEGER PRIMARY KEY, "PharmacyID" VARCHAR(30) NOT NULL, "PharmacyGLN" VARCHAR(20),
        "PharmacyName" VARCHAR(100), "PharmacyType" VARCHAR(30), "HospitalKey" INTEGER NOT NULL)""",

    "DimLocation": """CREATE TABLE IF NOT EXISTS star."DimLocation" (
        "LocationKey" INTEGER PRIMARY KEY, "Country" VARCHAR(10) NOT NULL, "State" VARCHAR(10) NOT NULL,
        "City" VARCHAR(100) NOT NULL)""",

    "DimSalesProfile": """CREATE TABLE IF NOT EXISTS star."DimSalesProfile" (
        "SalesProfileKey" INTEGER PRIMARY KEY, "ChannelGroup" VARCHAR(30) NOT NULL,
        "SalesChannel" VARCHAR(50) NOT NULL, "CustomerSegment" VARCHAR(50) NOT NULL,
        "DeliveryOTIFFlag" VARCHAR(10) NOT NULL)""",

    "FactSales": """CREATE TABLE IF NOT EXISTS star."FactSales" (
        "SalesTransactionID" VARCHAR(40) PRIMARY KEY,
        "DateKey" INTEGER NOT NULL, "MedKey" INTEGER NOT NULL, "PharmacyKey" INTEGER NOT NULL,
        "LocationKey" INTEGER NOT NULL, "SalesProfileKey" INTEGER NOT NULL,
        "QuantitySoldPacks" INTEGER,
        "UnitRetailPriceUSD" DECIMAL(10,2), "GrossSalesAmountUSD" DECIMAL(14,2),
        "DiscountAmountUSD" DECIMAL(14,2), "NetSalesAmountUSD" DECIMAL(14,2),
        "SalesTaxUSD" DECIMAL(14,2), "CostOfGoodsSoldUSD" DECIMAL(14,2),
        "GrossMarginUSD" DECIMAL(14,2), "DaysToExpiryAtSale" INTEGER)""",

    "FactDemand": """CREATE TABLE IF NOT EXISTS star."FactDemand" (
        "DemandRecordID" VARCHAR(60) PRIMARY KEY,
        "DateKey" INTEGER NOT NULL, "MedKey" INTEGER NOT NULL, "HospitalKey" INTEGER NOT NULL,
        "LocationKey" INTEGER NOT NULL,
        "EstimatedDiseaseCases" INTEGER, "DiseaseRelatedAdmissions" INTEGER,
        "OutpatientEncounters" INTEGER, "ForecastDemandPacks" INTEGER, "RequestedQuantityPacks" INTEGER,
        "ApprovedQuantityPacks" INTEGER, "ReceivedQuantityPacks" INTEGER,
        "EmergencyRequestQuantityPacks" INTEGER, "OpeningStockPacks" INTEGER,
        "DispensedQuantityPacks" INTEGER, "ConsumedQuantityPacks" INTEGER,
        "WastageQuantityPacks" INTEGER, "ClosingStockPacks" INTEGER, "UnfulfilledDemandPacks" INTEGER,
        "DemandTrendIndex" DECIMAL(6,3))""",
}


def export():
    # Read from DuckDB
    duck = duckdb.connect(WAREHOUSE, read_only=True)

    # Connect to PostgreSQL
    conn = psycopg2.connect(**PG)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # Create star schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS star")

        for table in TABLES:
            log.info("Exporting %s ...", table)

            # Get data from DuckDB
            cols_info = duck.execute(f"SELECT * FROM {table} LIMIT 0").description
            col_names = [c[0] for c in cols_info]
            rows = duck.execute(f"SELECT * FROM {table}").fetchall()

            # Drop and recreate in PostgreSQL
            cur.execute(f'DROP TABLE IF EXISTS star."{table}" CASCADE')
            cur.execute(PG_DDL[table])

            if rows:
                # Build INSERT
                quoted_cols = ', '.join(f'"{c}"' for c in col_names)
                template = f'INSERT INTO star."{table}" ({quoted_cols}) VALUES %s'
                # Convert rows: handle Python date/bool objects
                clean_rows = []
                for row in rows:
                    clean_row = []
                    for val in row:
                        if val is None:
                            clean_row.append(None)
                        else:
                            clean_row.append(val)
                    clean_rows.append(tuple(clean_row))

                # Batch insert (1000 rows at a time for memory)
                batch_size = 5000
                for i in range(0, len(clean_rows), batch_size):
                    batch = clean_rows[i:i + batch_size]
                    execute_values(cur, template, batch, page_size=batch_size)

            row_count = len(rows)
            log.info("  %s: %s rows exported", table, f"{row_count:,}")

        conn.commit()
        log.info("All tables exported to PostgreSQL star schema successfully")

    except Exception as e:
        conn.rollback()
        log.error("Export failed: %s", e)
        raise
    finally:
        cur.close()
        conn.close()
        duck.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", stream=sys.stdout)
    try:
        export()
    except Exception as e:
        log.error("EXPORT FAILED: %s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
