#!/usr/bin/env python3
"""
validate_staging.py  -  data-quality gate between staging and the star schema.

Runs SQL checks on stg_master / stg_sales / stg_demand and writes one row per check to
validation_results. ERROR checks that fail stop the pipeline (exit code 1); WARN checks are
only recorded. Nothing in the staging tables is changed.
"""
import argparse
import os
import re
import sys

import psycopg2

PG = dict(host=os.getenv("PGHOST", "localhost"), port=int(os.getenv("PGPORT", "5433")),
          user=os.getenv("PGUSER", "postgres"), password=os.getenv("PGPASSWORD", "postgres"),
          dbname=os.getenv("PGDATABASE", "postgres"))
SCHEMA = os.getenv("PG_SCHEMA", "public")
TABLES = {"m": os.getenv("STG_MASTER", "stg_master"), "s": os.getenv("STG_SALES", "stg_sales"),
          "d": os.getenv("STG_DEMAND", "stg_demand")}
RESULT_COLS = ["batch_id", "table_name", "check_name", "severity", "status", "failed_rows",
               "description", "checked_at"]
RESULT_DDL = """CREATE TABLE validation_results (batch_id TEXT, table_name TEXT, check_name TEXT,
                severity TEXT, status TEXT, failed_rows BIGINT, description TEXT,
                checked_at TIMESTAMP DEFAULT now())"""
NUM_RE = r"^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$"


def _n(s):
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


class Table:
    """A staging table. t.c('City') returns the real, quoted column name (aliases allowed)."""
    ALIASES = {"City": ["PharmacyCity", "HospitalCity"], "State": ["PharmacyState", "HospitalState"]}

    def __init__(self, cur, key):
        self.name = TABLES[key]
        self.full = f'"{SCHEMA}"."{self.name}"'
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
                    (SCHEMA, self.name))
        self.cols = {_n(r[0]): r[0] for r in cur.fetchall()}
        if not self.cols:
            raise SystemExit(f"ERROR: staging table {self.name} not found. Run load_staging.py first.")

    def c(self, name):
        for cand in [name] + self.ALIASES.get(name, []):
            if _n(cand) in self.cols:
                return '"' + self.cols[_n(cand)] + '"'
        raise KeyError(name)


def blank(col):
    return f"({col} IS NULL OR btrim({col}) = '')"


def num(col):
    return f"(CASE WHEN btrim({col}) ~ '{NUM_RE}' THEN btrim({col})::numeric END)"


def not_num(col):
    return f"({col} IS NULL OR btrim({col}) !~ '{NUM_RE}')"


def gtin(col):
    return f"lpad(regexp_replace(btrim({col}), '\\.0$', ''), 14, '0')"


def dups(t, col):  # number of rows that share their value with another row
    return f"SELECT COALESCE(SUM(n),0) FROM (SELECT COUNT(*) n FROM {t.full} GROUP BY {col} HAVING COUNT(*) > 1) x"


def count_where(t, cond):
    return f"SELECT COUNT(*) FROM {t.full} WHERE {cond}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-id", default=None)
    batch = ap.parse_args().batch_id
    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f'SET search_path TO "{SCHEMA}"')

    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name='validation_results'",
                (SCHEMA,))
    have = [r[0] for r in cur.fetchall()]
    if have != RESULT_COLS:
        if have:
            cur.execute("DROP TABLE validation_results")
        cur.execute(RESULT_DDL)
    if batch is None:
        try:
            cur.execute("SELECT batch_id FROM load_metrics ORDER BY loaded_at DESC LIMIT 1")
            row = cur.fetchone()
            batch = row[0] if row else "adhoc"
        except psycopg2.Error:
            batch = "adhoc"

    m, s, d = Table(cur, "m"), Table(cur, "s"), Table(cur, "d")
    results = []

    def run(table, name, severity, desc, build):
        try:
            cur.execute(build())
            failed = int(cur.fetchone()[0] or 0)
            status = "PASS" if failed == 0 else ("FAIL" if severity == "ERROR" else "WARN")
        except KeyError as e:
            failed, status, desc = None, "SKIPPED", f"{desc} (column {e.args[0]} not found)"
        except psycopg2.Error as e:
            failed, status, desc = None, "FAIL", f"{desc} (SQL error: {str(e).splitlines()[0]})"
        cur.execute("INSERT INTO validation_results (batch_id, table_name, check_name, severity, status, failed_rows, description) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)", (batch, table.name, name, severity, status, failed, desc))
        results.append((table.name, name, severity, status, failed))

    # ---- reconcile: table still has the rows load_staging reported -------------------------
    for t in (m, s, d):
        def recon(t=t):
            cur.execute("SELECT table_rows_after FROM load_metrics WHERE table_name=%s ORDER BY loaded_at DESC LIMIT 1", (t.name,))
            r = cur.fetchone()
            if r is None:
                raise KeyError("load_metrics row")
            return f"SELECT ABS(COUNT(*) - {int(r[0])}) FROM {t.full}"
        run(t, "reconcile_row_count", "ERROR", "table rows = rows reported by load_staging", recon)
        run(t, "has_rows", "ERROR", "table is not empty", lambda t=t: f"SELECT CASE WHEN COUNT(*)=0 THEN 1 ELSE 0 END FROM {t.full}")

    # ---- master ----------------------------------------------------------------------------
    run(m, "sku_not_blank", "ERROR", "InternalSKU is filled", lambda: count_where(m, blank(m.c("InternalSKU"))))
    run(m, "sku_unique", "ERROR", "InternalSKU is unique", lambda: dups(m, m.c("InternalSKU")))
    run(m, "gtin_unique", "ERROR", "GTIN14 is unique", lambda: dups(m, gtin(m.c("GTIN14"))))
    run(m, "numeric_columns", "ERROR", "pack size, shelf life and prices are numbers",
        lambda: count_where(m, " OR ".join(not_num(m.c(c)) for c in
                                           ["PackSize", "ShelfLifeMonths", "UnitManufacturingCostUSD",
                                            "WholesalePriceUSD", "RecommendedRetailPriceUSD"])))
    run(m, "price_order", "WARN", "cost <= wholesale <= retail price",
        lambda: count_where(m, f"{num(m.c('UnitManufacturingCostUSD'))} > {num(m.c('WholesalePriceUSD'))} OR "
                               f"{num(m.c('WholesalePriceUSD'))} > {num(m.c('RecommendedRetailPriceUSD'))}"))

    # ---- sales -----------------------------------------------------------------------------
    run(s, "id_unique", "ERROR", "SalesTransactionID is filled and unique",
        lambda: f"SELECT (SELECT COUNT(*) FROM {s.full} WHERE {blank(s.c('SalesTransactionID'))}) + "
                f"({dups(s, s.c('SalesTransactionID'))})")
    run(s, "date_format", "ERROR", "SalesDate looks like YYYY-MM-DD",
        lambda: count_where(s, f"{s.c('SalesDate')} IS NULL OR btrim({s.c('SalesDate')}) !~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'"))
    run(s, "numeric_columns", "ERROR", "quantity, prices, amounts and expiry days are numbers",
        lambda: count_where(s, " OR ".join(not_num(s.c(c)) for c in
                                           ["QuantitySoldPacks", "UnitRetailPriceUSD", "GrossSalesAmountUSD",
                                            "DiscountAmountUSD", "NetSalesAmountUSD", "SalesTaxUSD",
                                            "CostOfGoodsSoldUSD", "DaysToExpiryAtSale"])))
    run(s, "key_columns_filled", "ERROR", "PharmacyID, GTIN14, city and state are filled",
        lambda: count_where(s, " OR ".join(blank(s.c(c)) for c in ["PharmacyID", "GTIN14", "City", "State"])))
    run(s, "gtin_in_master", "ERROR", "every sold GTIN14 exists in master data",
        lambda: f"SELECT COUNT(*) FROM {s.full} x WHERE NOT EXISTS (SELECT 1 FROM {m.full} y WHERE "
                f"{gtin('y.' + m.c('GTIN14'))} = {gtin('x.' + s.c('GTIN14'))})")
    run(s, "hospital_known", "ERROR", "a pharmacy's HospitalID (if any) exists in the demand data",
        lambda: f"SELECT COUNT(*) FROM {s.full} x WHERE NOT {blank('x.' + s.c('HospitalID'))} AND NOT EXISTS "
                f"(SELECT 1 FROM {d.full} y WHERE btrim(y.{d.c('HospitalID')}) = btrim(x.{s.c('HospitalID')}))")
    run(s, "margin_rule", "ERROR", "GrossMarginUSD = Net sales - COGS (1 cent tolerance)",
        lambda: count_where(s, f"ABS({num(s.c('GrossMarginUSD'))} - ({num(s.c('NetSalesAmountUSD'))} - "
                               f"{num(s.c('CostOfGoodsSoldUSD'))})) > 0.01"))
    run(s, "net_rule", "WARN", "Gross - Discount = Net (1 cent tolerance)",
        lambda: count_where(s, f"ABS({num(s.c('GrossSalesAmountUSD'))} - {num(s.c('DiscountAmountUSD'))} - "
                               f"{num(s.c('NetSalesAmountUSD'))}) > 0.01"))
    run(s, "quantity_positive", "WARN", "QuantitySoldPacks > 0",
        lambda: count_where(s, f"{num(s.c('QuantitySoldPacks'))} <= 0"))
    run(s, "pharmacy_attrs_constant", "WARN", "each pharmacy has one name, type, GLN and hospital",
        lambda: f"SELECT COUNT(*) FROM (SELECT 1 FROM {s.full} GROUP BY {s.c('PharmacyID')} HAVING "
                f"COUNT(DISTINCT concat_ws('|', {s.c('PharmacyGLN')}, {s.c('PharmacyName')}, {s.c('PharmacyType')}, "
                f"{s.c('HospitalID')})) > 1) x")

    # ---- demand ----------------------------------------------------------------------------
    num_cols = ["LicensedBeds", "ICUBeds", "AverageOccupancyRatePct", "DemandTrendIndex", "EstimatedDiseaseCases",
                "DiseaseRelatedAdmissions", "OutpatientEncounters", "ForecastDemandPacks", "RequestedQuantityPacks",
                "ApprovedQuantityPacks", "ReceivedQuantityPacks", "EmergencyRequestQuantityPacks", "OpeningStockPacks",
                "DispensedQuantityPacks", "ConsumedQuantityPacks", "WastageQuantityPacks", "UnfulfilledDemandPacks"]
    run(d, "month_format", "ERROR", "DemandMonth looks like YYYY-MM",
        lambda: count_where(d, f"{d.c('DemandMonth')} IS NULL OR btrim({d.c('DemandMonth')}) !~ '^[0-9]{{4}}-[0-9]{{2}}'"))
    run(d, "numeric_columns", "ERROR", "beds, occupancy, stock and demand columns are numbers",
        lambda: count_where(d, " OR ".join(not_num(d.c(c)) for c in num_cols)))
    run(d, "grain_unique", "ERROR", "one row per hospital + medicine + month",
        lambda: dups(d, f"{d.c('HospitalID')}, {gtin(d.c('GTIN14'))}, {d.c('DemandMonth')}"))
    run(d, "gtin_in_master", "ERROR", "every demand GTIN14 exists in master data",
        lambda: f"SELECT COUNT(*) FROM {d.full} x WHERE NOT EXISTS (SELECT 1 FROM {m.full} y WHERE "
                f"{gtin('y.' + m.c('GTIN14'))} = {gtin('x.' + d.c('GTIN14'))})")
    run(d, "closing_stock_rule", "ERROR", "Closing = Opening + Received - Dispensed - Wastage",
        lambda: count_where(d, f"{num(d.c('ClosingStockPacks'))} <> {num(d.c('OpeningStockPacks'))} + "
                               f"{num(d.c('ReceivedQuantityPacks'))} - {num(d.c('DispensedQuantityPacks'))} - "
                               f"{num(d.c('WastageQuantityPacks'))}"))
    run(d, "hospital_attrs_constant", "WARN", "each hospital has one name, type, beds and occupancy",
        lambda: f"SELECT COUNT(*) FROM (SELECT 1 FROM {d.full} GROUP BY {d.c('HospitalID')} HAVING "
                f"COUNT(DISTINCT concat_ws('|', {d.c('HospitalGLN')}, {d.c('HospitalName')}, {d.c('HospitalType')}, "
                f"{d.c('LicensedBeds')}, {d.c('ICUBeds')}, {d.c('AverageOccupancyRatePct')})) > 1) x")

    print(f"validation batch '{batch}'")
    print(f"{'table':12s} {'check':26s} {'sev':5s} {'status':8s} failed_rows")
    for t, name, sev, status, failed in results:
        print(f"{t:12s} {name:26s} {sev:5s} {status:8s} {'' if failed is None else f'{failed:,}'}")
    bad = [r for r in results if r[3] == "FAIL"]
    warn = [r for r in results if r[3] == "WARN"]
    print(f"\n{len(results)} checks: {len(bad)} FAIL, {len(warn)} WARN")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
