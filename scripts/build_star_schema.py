#!/usr/bin/env python3
"""
build_star_schema.py  -  Postgres staging  ->  DuckDB star schema (SDI data mart)

based on DWH_Template_FINALBOSS.xlsx (Dim.Tables, Fact.Table, Transform.OLTP).

design:
Read the 3 staging tables, clean + validate them in pandas (nothing touches the
warehouse yet), then load everything in ONE DuckDB transaction: if any check
fails the warehouse is rolled back untouched. The same script does the first
load and every refresh: dimensions are merged (SCD0 / SCD1 / SCD2 as the
template says), facts are appended only if their ID is new. Run it twice on the
same data and nothing changes (idempotent).

usage:
  python build_star_schema.py                  # first load or refresh
  python build_star_schema.py --full-rebuild   # drop everything, load again
  python build_star_schema.py --effective-date 2026-01-01   # SCD2 date for price changes
"""

import argparse
import gc 
import logging
import os
import re
import sys
from datetime import date, timedelta

import duckdb
import pandas as pd

# CONFIG 
PG = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5433")),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", "postgres"),
    dbname=os.getenv("PGDATABASE", "postgres"),
)
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")
SRC = {  # staging table names
    "master": os.getenv("STG_MASTER", "stg_master"),
    "sales": os.getenv("STG_SALES", "stg_sales"),
    "demand": os.getenv("STG_DEMAND", "stg_demand"),
}
WAREHOUSE = os.getenv("WAREHOUSE_DB", "warehouse.duckdb")

DATE_START, DATE_END = date(2023, 1, 1), date(2025, 12, 31)  # DimDate: 1,096 days
FIRST_VALID_FROM = date(2023, 1, 1)                            # SCD2 first version
NO_HOSPITAL_KEY = 0

# Row counts the template promises for the FIRST load (checked, warning only)
EXPECTED_FIRST_LOAD = {
    "DimHospital": 13, "DimMedicine": 63, "DimPharmacy": 60, "DimSalesProfile": 30,
    "DimDate": 1096, "FactSales": 500_000, "FactDemand": 27_216,
}

log = logging.getLogger("star")


class BuildError(Exception):
    """Any data-quality or schema problem. The run stops and the warehouse is rolled back."""


# DDL  (column names, order and types follow Dim.Tables / Fact.Table)

DDL = {
    "DimDate": """CREATE TABLE IF NOT EXISTS DimDate (
        DateKey INTEGER PRIMARY KEY, FullDate DATE NOT NULL, Year INTEGER, Quarter INTEGER,
        Month INTEGER, MonthName VARCHAR(10), YearMonth CHAR(7), DayOfMonth INTEGER,
        DayName VARCHAR(10), IsWeekend BOOLEAN, Season VARCHAR(6))""",
    "DimHospital": """CREATE TABLE IF NOT EXISTS DimHospital (
        HospitalKey INTEGER PRIMARY KEY, HospitalID VARCHAR(20) NOT NULL, HospitalGLN CHAR(13),
        HospitalName VARCHAR(100), HospitalType VARCHAR(30), LicensedBeds INTEGER,
        ICUBeds INTEGER, OccupancyPct DECIMAL(5,2))""",
    "DimMedicine": """CREATE TABLE IF NOT EXISTS DimMedicine (
        MedKey INTEGER PRIMARY KEY, GTIN14 CHAR(14) NOT NULL, InternalSKU VARCHAR(20) NOT NULL,
        MedName VARCHAR(100), TherapeuticClass VARCHAR(60), ICD10Category VARCHAR(10),
        DiseaseName VARCHAR(60), DosageFormStrength VARCHAR(40), Route VARCHAR(20),
        PackSize INTEGER, PackSizeGroup VARCHAR(5), BaseUnitOfMeasure VARCHAR(10),
        PrescClass VARCHAR(10), ColdChainFlag VARCHAR(12), ShelfLifeMonths INTEGER,
        ShelfLifeGroup CHAR(2), ProductStatus VARCHAR(12), StandardCost DECIMAL(10,2),
        ListTradePrice DECIMAL(10,2), MRP DECIMAL(10,2), ValidFrom DATE NOT NULL,
        ValidTo DATE, IsCurrent BOOLEAN NOT NULL)""",
    "DimPharmacy": """CREATE TABLE IF NOT EXISTS DimPharmacy (
        PharmacyKey INTEGER PRIMARY KEY, PharmacyID VARCHAR(20) NOT NULL, PharmacyGLN CHAR(13),
        PharmacyName VARCHAR(30), PharmacyType VARCHAR(10), HospitalKey INTEGER NOT NULL)""",
    "DimLocation": """CREATE TABLE IF NOT EXISTS DimLocation (
        LocationKey INTEGER PRIMARY KEY, Country VARCHAR(2) NOT NULL, State VARCHAR(2) NOT NULL,
        City VARCHAR(60) NOT NULL)""",
    "DimSalesProfile": """CREATE TABLE IF NOT EXISTS DimSalesProfile (
        SalesProfileKey INTEGER PRIMARY KEY, ChannelGroup VARCHAR(12) NOT NULL,
        SalesChannel VARCHAR(30) NOT NULL, CustomerSegment VARCHAR(30) NOT NULL,
        DeliveryOTIFFlag VARCHAR(5) NOT NULL)""",
    "FactSales": """CREATE TABLE IF NOT EXISTS FactSales (
        DateKey INTEGER NOT NULL, MedKey INTEGER NOT NULL, PharmacyKey INTEGER NOT NULL,
        LocationKey INTEGER NOT NULL, SalesProfileKey INTEGER NOT NULL,
        SalesTransactionID VARCHAR(20) PRIMARY KEY, QuantitySoldPacks INTEGER,
        UnitRetailPriceUSD DECIMAL(10,2), GrossSalesAmountUSD DECIMAL(14,2),
        DiscountAmountUSD DECIMAL(14,2), NetSalesAmountUSD DECIMAL(14,2),
        SalesTaxUSD DECIMAL(14,2), CostOfGoodsSoldUSD DECIMAL(14,2),
        GrossMarginUSD DECIMAL(14,2), DaysToExpiryAtSale INTEGER)""",
    "FactDemand": """CREATE TABLE IF NOT EXISTS FactDemand (
        DateKey INTEGER NOT NULL, MedKey INTEGER NOT NULL, HospitalKey INTEGER NOT NULL,
        LocationKey INTEGER NOT NULL, DemandRecordID VARCHAR(40) PRIMARY KEY,
        EstimatedDiseaseCases INTEGER, DiseaseRelatedAdmissions INTEGER,
        OutpatientEncounters INTEGER, ForecastDemandPacks INTEGER, RequestedQuantityPacks INTEGER,
        ApprovedQuantityPacks INTEGER, ReceivedQuantityPacks INTEGER,
        EmergencyRequestQuantityPacks INTEGER, OpeningStockPacks INTEGER,
        DispensedQuantityPacks INTEGER, ConsumedQuantityPacks INTEGER,
        WastageQuantityPacks INTEGER, ClosingStockPacks INTEGER, UnfulfilledDemandPacks INTEGER,
        DemandTrendIndex DECIMAL(6,3))""",
}
TABLE_ORDER = ["DimDate", "DimHospital", "DimMedicine", "DimPharmacy", "DimLocation",
               "DimSalesProfile", "FactSales", "FactDemand"]


def _n(s):
    """Normalise a column name so 'Sales Date', 'sales_date', 'SalesDate' all match."""
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def standardize(df, required, optional=(), aliases=None, label=""):
    """Return df with canonical column names. Fail early and clearly if a column is missing."""
    aliases = aliases or {}
    lookup = {_n(c): c for c in df.columns}
    found, missing = {}, []
    for want in list(required) + list(optional):
        hit = next((lookup[_n(c)] for c in [want] + aliases.get(want, []) if _n(c) in lookup), None)
        if hit is not None:
            found[want] = hit
        elif want in required:
            missing.append(want)
    if missing:
        raise BuildError(
            f"{label}: missing columns {missing}.\n  Columns found: {sorted(df.columns)}\n"
            f"  Fix the name in the REQUIRED list / ALIASES of build_star_schema.py.")
    return df[list(found.values())].rename(columns={v: k for k, v in found.items()})


def clean_str(s):
    """Strip text; empty / 'nan' / 'none' / 'null' become real nulls."""
    s = s.astype("string").str.strip()
    return s.mask(s.str.lower().isin(["", "nan", "none", "null", "<na>"]))


def to_num(s, label, integer=False):
    out = pd.to_numeric(s, errors="coerce")
    bad = out.isna().sum()
    if bad:
        raise BuildError(f"{label}: {bad} rows are empty or not numeric.")
    return out.round(0).astype("int64") if integer else out.astype("float64")


def gtin14(s):
    # leading zeros are lost if the CSV was read as a number -> restore them
    return clean_str(s).str.replace(r"\.0$", "", regex=True).str.zfill(14)


def obj(s):
    """pandas string -> plain python objects (None for null) so DuckDB gets clean NULLs."""
    return s.astype(object).where(s.notna(), None)


def need_unique(df, cols, label):
    d = df.duplicated(cols, keep=False)
    if d.any():
        raise BuildError(f"{label}: {int(d.sum())} rows share the same {cols}. Example: "
                         f"{df.loc[d, cols].head(3).to_dict('records')}")


def count(con, table):
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchall()[0][0]


def insert_df(con, table, df):
    if df.empty:
        return 0
    con.register("_ins", df)
    cols = ", ".join(df.columns)
    con.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM _ins")
    con.unregister("_ins")
    return len(df)


# ----------------------------------------------------------------------------
# Transform rules from Transform.OLTP
# ----------------------------------------------------------------------------
# Rule 3: fixed FDA-style lookup (same result on every run)
DOSAGE_ABBR = {
    "tablet": "TAB", "capsule": "CAP", "injection": "INJ", "solution": "SOL",
    "suspension": "SUSP", "syrup": "SYR", "cream": "CRM", "ointment": "OINT", "gel": "GEL",
    "patch": "PCH", "inhaler": "INH", "spray": "SPR", "drops": "DRP", "powder": "PWD",
    "suppository": "SUPP", "lozenge": "LOZ", "vial": "VIAL", "prefilled syringe": "PFS",
    "pen injector": "PEN", "infusion": "INF", "emulsion": "EMUL", "elixir": "ELIX",
    "liquid": "LIQ", "lotion": "LOT", "solution for injection": "INJ",
    # exact forms found in the real master data (edit here if your template uses other short forms)
    "modified-release tablet": "TAB MR", "effervescent tablet": "TAB EFF",
    "gastro-resistant tablet": "TAB EC", "gastro-resistant capsule": "CAP EC",
    "inhalation capsule": "CAP INH", "dry-powder inhaler": "DPI", "metered-dose inhaler": "MDI",
    "nebulizer solution": "SOL NEB", "oral solution": "SOL", "pre-filled pen": "PEN",
}
_unmapped_forms = set()


def abbreviate_form(form):
    if form is None or pd.isna(form):
        return ""
    key = str(form).strip().lower()
    if key in DOSAGE_ABBR:
        return DOSAGE_ABBR[key]
    first = key.split()[0] if key.split() else key
    if first in DOSAGE_ABBR:
        return DOSAGE_ABBR[first]
    _unmapped_forms.add(str(form))
    return re.sub(r"[^A-Za-z]", "", str(form)).upper()[:4]


def abbreviate_pharmacy_name(name):
    """Rule 6: 'WellSpring Pharmacy 40' -> 'WS-40' (capitals before 'Pharmacy' + '-' + number)."""
    if name is None or pd.isna(name):
        return None
    m = re.match(r"^(.*?)\s*pharmacy\s*(\d+)\s*$", str(name).strip(), flags=re.I)
    prefix, number = (m.group(1), m.group(2)) if m else (str(name), "".join(re.findall(r"\d+", str(name))))
    caps = "".join(re.findall(r"[A-Z]", prefix)) or prefix[:1].upper()
    return f"{caps}-{number}" if number else caps


def abbreviate_pharmacy_type(t):
    """Rule 7: initials of each word with dots. 'Chain Pharmacy' -> 'C.P.'"""
    if t is None or pd.isna(t):
        return None
    return "".join(w[0].upper() + "." for w in str(t).split())


def derive_channel_group(channel, pharmacy_type):
    """Rule 9: Retail / Hospital / E-commerce from SalesChannel and pharmacy type."""
    c = str(channel).lower()
    t = str(pharmacy_type).lower()
    if any(k in c for k in ("e-com", "ecom", "online", "web", "digital")):
        return "E-commerce"
    if any(k in c for k in ("hospital", "tender", "institution")) or "hospital" in t:
        return "Hospital"
    return "Retail"


def season(month):
    return {12: "Winter", 1: "Winter", 2: "Winter", 3: "Spring", 4: "Spring", 5: "Spring",
            6: "Summer", 7: "Summer", 8: "Summer"}.get(month, "Autumn")


#  read staging

def read_sources():
    try:
        import psycopg2
    except ImportError:
        raise BuildError("psycopg2 is missing: pip install psycopg2-binary")
    specs = {
        "master": (MASTER_REQ, MASTER_OPT, MASTER_ALIAS),
        "sales": (SALES_REQ, SALES_OPT, SALES_ALIAS),
        "demand": (DEMAND_REQ, DEMAND_OPT, DEMAND_ALIAS),
    }
    out = {}
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        for key, table in SRC.items():
            cur.execute(f'SELECT * FROM "{PG_SCHEMA}"."{table}" LIMIT 0')
            pg_cols = {_n(d[0]): d[0] for d in cur.description}
            req, opt, aliases = specs[key]
            want = [pg_cols[_n(c)] for w in req + opt for c in [w] + aliases.get(w, []) if _n(c) in pg_cols]
            col_sql = ", ".join(f'"{c}"' for c in dict.fromkeys(want)) or "*"
            cur.execute(f'SELECT {col_sql} FROM "{PG_SCHEMA}"."{table}"')
            out[key] = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
            log.info("staging  %-26s %9s rows", table, f"{len(out[key]):,}")
    return out 


# prepare + validate (pure pandas, no warehouse access)

MASTER_REQ = ["InternalSKU", "GTIN14", "GenericName", "ICD10Category", "DiseaseCategory",
              "TherapeuticClass", "DosageForm", "Strength", "RouteOfAdministration", "PackSize",
              "BaseUnitOfMeasure", "PrescriptionClass", "ColdChainFlag", "ShelfLifeMonths",
              "UnitManufacturingCostUSD", "WholesalePriceUSD", "RecommendedRetailPriceUSD"]
MASTER_OPT = ["ProductStatus"]
MASTER_ALIAS = {"DosageForm": ["Dosage Form", "Form"], "Strength": ["StrengthValue", "DrugStrength"]}

SALES_REQ = ["SalesTransactionID", "SalesDate", "PharmacyID", "PharmacyGLN", "PharmacyName",
             "PharmacyType", "City", "State", "HospitalID", "SalesChannel", "CustomerSegment",
             "DeliveryOTIFFlag", "QuantitySoldPacks", "UnitRetailPriceUSD", "GrossSalesAmountUSD",
             "DiscountAmountUSD", "NetSalesAmountUSD", "SalesTaxUSD", "CostOfGoodsSoldUSD",
             "DaysToExpiryAtSale"]
SALES_OPT = ["GTIN14", "InternalSKU", "GrossMarginUSD"]
SALES_ALIAS = {"City": ["PharmacyCity"], "State": ["PharmacyState"]}

DEMAND_REQ = ["DemandMonth", "HospitalID", "HospitalGLN", "HospitalName", "HospitalType",
              "LicensedBeds", "ICUBeds", "AverageOccupancyRatePct", "GTIN14", "City", "State",
              "EstimatedDiseaseCases", "DiseaseRelatedAdmissions", "OutpatientEncounters",
              "ForecastDemandPacks", "RequestedQuantityPacks", "ApprovedQuantityPacks",
              "ReceivedQuantityPacks", "EmergencyRequestQuantityPacks", "OpeningStockPacks",
              "DispensedQuantityPacks", "ConsumedQuantityPacks", "WastageQuantityPacks",
              "UnfulfilledDemandPacks", "DemandTrendIndex"]
DEMAND_OPT = ["DemandRecordID", "ClosingStockPacks"]
DEMAND_ALIAS = {"City": ["HospitalCity"], "State": ["HospitalState"]}


def check_schemas(src):
    """Report ALL missing columns of ALL three tables in one go (saves round trips)."""
    errs = []
    for key, req, opt, al in [("master", MASTER_REQ, MASTER_OPT, MASTER_ALIAS),
                              ("sales", SALES_REQ, SALES_OPT, SALES_ALIAS),
                              ("demand", DEMAND_REQ, DEMAND_OPT, DEMAND_ALIAS)]:
        try:
            standardize(src[key], req, opt, al, SRC[key])
        except BuildError as e:
            errs.append(str(e))
    if errs:
        raise BuildError("\n".join(errs))


def prep_medicine(raw):
    m = standardize(raw, MASTER_REQ, MASTER_OPT, MASTER_ALIAS, SRC["master"])
    m["InternalSKU"] = clean_str(m["InternalSKU"])
    m["GTIN14"] = gtin14(m["GTIN14"])
    need_unique(m, ["InternalSKU"], "master InternalSKU")
    need_unique(m, ["GTIN14"], "master GTIN14")
    form = m["DosageForm"].map(abbreviate_form)
    strength = clean_str(m["Strength"]).fillna("")
    pack = to_num(m["PackSize"], "PackSize", integer=True)
    shelf = to_num(m["ShelfLifeMonths"], "ShelfLifeMonths", integer=True)
    status = clean_str(m["ProductStatus"]).fillna("Active") if "ProductStatus" in m else "Active"
    out = pd.DataFrame({
        "GTIN14": obj(m["GTIN14"]), "InternalSKU": obj(m["InternalSKU"]),
        "MedName": obj(clean_str(m["GenericName"])),
        "TherapeuticClass": obj(clean_str(m["TherapeuticClass"])),
        "ICD10Category": obj(clean_str(m["ICD10Category"])),
        "DiseaseName": obj(clean_str(m["DiseaseCategory"])),
        "DosageFormStrength": (form + " " + strength).str.strip(),
        "Route": obj(clean_str(m["RouteOfAdministration"])),
        "PackSize": pack, "PackSizeGroup": pack.map(lambda x: "Large" if x > 15 else "Small"),
        "BaseUnitOfMeasure": obj(clean_str(m["BaseUnitOfMeasure"])),
        "PrescClass": obj(clean_str(m["PrescriptionClass"])),
        "ColdChainFlag": obj(clean_str(m["ColdChainFlag"])),
        "ShelfLifeMonths": shelf, "ShelfLifeGroup": shelf.map(lambda x: "SL" if x < 36 else "L"),
        "ProductStatus": obj(status) if not isinstance(status, str) else status,
        "StandardCost": to_num(m["UnitManufacturingCostUSD"], "StandardCost").round(2),
        "ListTradePrice": to_num(m["WholesalePriceUSD"], "ListTradePrice").round(2),
        "MRP": to_num(m["RecommendedRetailPriceUSD"], "MRP").round(2),
    }).sort_values("InternalSKU").reset_index(drop=True)
    if _unmapped_forms:
        log.warning("dosage forms NOT in the FDA lookup (first 4 letters used): %s", sorted(_unmapped_forms))
    log.info("dosage-form check: %s", dict(zip(m["DosageForm"].drop_duplicates().head(8),
                                               m["DosageForm"].drop_duplicates().head(8).map(abbreviate_form))))
    return out


def prep_sales(raw, medicine_src):
    s = standardize(raw, SALES_REQ, SALES_OPT, SALES_ALIAS, SRC["sales"])
    if "GTIN14" in s:
        s["GTIN14"] = gtin14(s["GTIN14"])
    elif "InternalSKU" in s:  # fall back: map SKU -> GTIN14 through the master data
        s["GTIN14"] = clean_str(s["InternalSKU"]).map(dict(zip(medicine_src["InternalSKU"], medicine_src["GTIN14"])))
    else:
        raise BuildError(f"{SRC['sales']}: needs GTIN14 or InternalSKU to find the medicine.")
    d = pd.DataFrame({
        "SalesTransactionID": clean_str(s["SalesTransactionID"]),
        "SalesDate": pd.to_datetime(s["SalesDate"], errors="coerce").dt.normalize(),
        "GTIN14": s["GTIN14"],
        "PharmacyID": clean_str(s["PharmacyID"]),
        "PharmacyGLN": clean_str(s["PharmacyGLN"]).str.replace(r"\.0$", "", regex=True).str.zfill(13),
        "PharmacyNameRaw": clean_str(s["PharmacyName"]),
        "PharmacyTypeRaw": clean_str(s["PharmacyType"]),
        "HospitalID": clean_str(s["HospitalID"]),
        "City": clean_str(s["City"]), "State": clean_str(s["State"]).str.upper(),
        "SalesChannel": clean_str(s["SalesChannel"]).fillna("N/A"),
        "CustomerSegment": clean_str(s["CustomerSegment"]).fillna("N/A"),
        "DeliveryOTIFFlag": clean_str(s["DeliveryOTIFFlag"]).fillna("N/A"),
        "QuantitySoldPacks": to_num(s["QuantitySoldPacks"], "QuantitySoldPacks", True),
        "UnitRetailPriceUSD": to_num(s["UnitRetailPriceUSD"], "UnitRetailPriceUSD").round(2),
        "GrossSalesAmountUSD": to_num(s["GrossSalesAmountUSD"], "GrossSalesAmountUSD").round(2),
        "DiscountAmountUSD": to_num(s["DiscountAmountUSD"], "DiscountAmountUSD").round(2),
        "NetSalesAmountUSD": to_num(s["NetSalesAmountUSD"], "NetSalesAmountUSD").round(2),
        "SalesTaxUSD": to_num(s["SalesTaxUSD"], "SalesTaxUSD").round(2),
        "CostOfGoodsSoldUSD": to_num(s["CostOfGoodsSoldUSD"], "CostOfGoodsSoldUSD").round(2),
        "DaysToExpiryAtSale": to_num(s["DaysToExpiryAtSale"], "DaysToExpiryAtSale", True),
    })
    for col in ["SalesTransactionID", "SalesDate", "GTIN14", "PharmacyID", "City", "State"]:
        if d[col].isna().any():
            raise BuildError(f"sales: {int(d[col].isna().sum())} rows have an empty {col}.")
    need_unique(d, ["SalesTransactionID"], "sales SalesTransactionID")

    # Validation: GrossMargin = Net - COGS (template rule)
    margin = (d["NetSalesAmountUSD"] - d["CostOfGoodsSoldUSD"]).round(2)
    if "GrossMarginUSD" in s:
        diff = (to_num(s["GrossMarginUSD"], "GrossMarginUSD") - margin).abs() > 0.01
        if diff.any():
            raise BuildError(f"sales: GrossMarginUSD != Net - COGS in {int(diff.sum())} rows "
                             f"(e.g. {d.loc[diff, 'SalesTransactionID'].head(3).tolist()}).")
    d["GrossMarginUSD"] = margin
    soft = ((d["GrossSalesAmountUSD"] - d["DiscountAmountUSD"] - d["NetSalesAmountUSD"]).abs() > 0.01).sum()
    if soft:
        log.warning("sales: Gross - Discount != Net in %s rows (kept as loaded)", f"{soft:,}")

    d["PharmacyName"] = d["PharmacyNameRaw"].map(abbreviate_pharmacy_name)
    d["PharmacyType"] = d["PharmacyTypeRaw"].map(abbreviate_pharmacy_type)
    d["ChannelGroup"] = [derive_channel_group(c, t) for c, t in zip(d["SalesChannel"], d["PharmacyTypeRaw"])]
    return d


def prep_demand(raw):
    r = standardize(raw, DEMAND_REQ, DEMAND_OPT, DEMAND_ALIAS, SRC["demand"])
    month = pd.to_datetime(r["DemandMonth"], format="mixed", errors="coerce")
    if month.isna().any():
        raise BuildError(f"demand: {int(month.isna().sum())} rows have an unreadable DemandMonth.")
    month = month.dt.to_period("M").dt.to_timestamp()           # always the 1st of the month
    d = pd.DataFrame({
        "DemandMonth": month,
        "HospitalID": clean_str(r["HospitalID"]),
        "HospitalGLN": clean_str(r["HospitalGLN"]).str.replace(r"\.0$", "", regex=True).str.zfill(13),
        "HospitalName": clean_str(r["HospitalName"]), "HospitalType": clean_str(r["HospitalType"]),
        "LicensedBeds": to_num(r["LicensedBeds"], "LicensedBeds", True),
        "ICUBeds": to_num(r["ICUBeds"], "ICUBeds", True),
        "OccupancyRaw": to_num(r["AverageOccupancyRatePct"], "AverageOccupancyRatePct"),
        "GTIN14": gtin14(r["GTIN14"]),
        "City": clean_str(r["City"]), "State": clean_str(r["State"]).str.upper(),
        "DemandTrendIndex": to_num(r["DemandTrendIndex"], "DemandTrendIndex").round(3),
    })
    measures = ["EstimatedDiseaseCases", "DiseaseRelatedAdmissions", "OutpatientEncounters",
                "ForecastDemandPacks", "RequestedQuantityPacks", "ApprovedQuantityPacks",
                "ReceivedQuantityPacks", "EmergencyRequestQuantityPacks", "OpeningStockPacks",
                "DispensedQuantityPacks", "ConsumedQuantityPacks", "WastageQuantityPacks",
                "UnfulfilledDemandPacks"]
    for c in measures:
        d[c] = to_num(r[c], c, True)
    for col in ["HospitalID", "GTIN14", "City", "State"]:
        if d[col].isna().any():
            raise BuildError(f"demand: {int(d[col].isna().sum())} rows have an empty {col}.")

    closing = d["OpeningStockPacks"] + d["ReceivedQuantityPacks"] - d["DispensedQuantityPacks"] - d["WastageQuantityPacks"]
    if "ClosingStockPacks" in r:
        diff = to_num(r["ClosingStockPacks"], "ClosingStockPacks", True) != closing
        if diff.any():
            raise BuildError(f"demand: ClosingStock != Opening + Received - Dispensed - Wastage in {int(diff.sum())} rows.")
    d["ClosingStockPacks"] = closing

    if "DemandRecordID" in r:
        d["DemandRecordID"] = clean_str(r["DemandRecordID"])
    else:  # deterministic id: one row per hospital + medicine + month (the fact grain)
        d["DemandRecordID"] = d["HospitalID"] + "|" + d["GTIN14"] + "|" + d["DemandMonth"].dt.strftime("%Y%m")
    need_unique(d, ["DemandRecordID"], "demand DemandRecordID")
    need_unique(d, ["HospitalID", "GTIN14", "DemandMonth"], "demand grain (hospital, medicine, month)")
    return d


# load dimensions

def load_dim_date(con, sales, demand):
    lo = min(DATE_START, sales["SalesDate"].min().date(), demand["DemandMonth"].min().date())
    hi = max(DATE_END, sales["SalesDate"].max().date(), demand["DemandMonth"].max().date())
    d = pd.DataFrame({"FullDate": pd.date_range(lo, hi, freq="D")})
    d["DateKey"] = d.FullDate.dt.year * 10000 + d.FullDate.dt.month * 100 + d.FullDate.dt.day
    d["Year"], d["Month"] = d.FullDate.dt.year, d.FullDate.dt.month
    d["Quarter"] = d.FullDate.dt.quarter
    d["MonthName"], d["YearMonth"] = d.FullDate.dt.month_name(), d.FullDate.dt.strftime("%Y-%m")
    d["DayOfMonth"], d["DayName"] = d.FullDate.dt.day, d.FullDate.dt.day_name()
    d["IsWeekend"] = d.FullDate.dt.dayofweek >= 5
    d["Season"] = d.Month.map(season)
    d["FullDate"] = d.FullDate.dt.date
    have = {r[0] for r in con.execute("SELECT DateKey FROM DimDate").fetchall()}
    new = d[~d.DateKey.isin(have)][["DateKey", "FullDate", "Year", "Quarter", "Month", "MonthName",
                                     "YearMonth", "DayOfMonth", "DayName", "IsWeekend", "Season"]]
    n = insert_df(con, "DimDate", new)
    if have and n:
        log.info("DimDate extended by %d days (new data goes beyond the old range)", n)
    return n


def load_dim_hospital(con, demand):
    con.execute("""INSERT INTO DimHospital VALUES (0, 'NO_HOSPITAL', NULL, 'No hospital', 'N/A', NULL, NULL, NULL)
                   ON CONFLICT DO NOTHING""")
    g = demand.groupby("HospitalID", sort=True)
    for c in ["HospitalGLN", "HospitalName", "HospitalType", "LicensedBeds", "ICUBeds", "OccupancyRaw"]:
        varying = (g[c].nunique() > 1).sum()
        if varying:
            log.warning("DimHospital: %s changes inside %d hospital(s); first value used", c, varying)
    h = g[["HospitalGLN", "HospitalName", "HospitalType", "LicensedBeds", "ICUBeds", "OccupancyRaw"]].first().reset_index()
    if h["OccupancyRaw"].max() <= 1.0:      # source is a ratio (0.78) but the column is a percent
        h["OccupancyRaw"] = h["OccupancyRaw"] * 100
        log.info("DimHospital: occupancy looked like a 0-1 ratio, multiplied by 100 to make OccupancyPct")
    h["OccupancyPct"] = h["OccupancyRaw"].round(2)
    h = h.drop(columns="OccupancyRaw")
    con.register("src_h", h)
    # SCD1: overwrite changed attributes (HospitalID / GLN / Name are SCD0, never touched)
    con.execute("""UPDATE DimHospital SET HospitalType = s.HospitalType, LicensedBeds = s.LicensedBeds,
                          ICUBeds = s.ICUBeds, OccupancyPct = CAST(s.OccupancyPct AS DECIMAL(5,2))
                   FROM src_h s WHERE DimHospital.HospitalID = s.HospitalID AND
                        (DimHospital.HospitalType IS DISTINCT FROM s.HospitalType OR
                         DimHospital.LicensedBeds IS DISTINCT FROM s.LicensedBeds OR
                         DimHospital.ICUBeds IS DISTINCT FROM s.ICUBeds OR
                         DimHospital.OccupancyPct IS DISTINCT FROM CAST(s.OccupancyPct AS DECIMAL(5,2)))""")
    con.unregister("src_h")
    have = {r[0] for r in con.execute("SELECT HospitalID FROM DimHospital").fetchall()}
    new = h[~h.HospitalID.isin(have)].copy()
    new.insert(0, "HospitalKey", range(next_key(con, "DimHospital", "HospitalKey"),
                                       next_key(con, "DimHospital", "HospitalKey") + len(new)))
    return insert_df(con, "DimHospital", new)


def next_key(con, table, key):
    return con.execute(f"SELECT COALESCE(MAX({key}), 0) + 1 FROM {table}").fetchall()[0][0]


def load_dim_medicine(con, med, effective):
    scd1 = ["MedName", "TherapeuticClass", "ICD10Category", "DiseaseName", "DosageFormStrength", "Route",
            "PackSize", "PackSizeGroup", "BaseUnitOfMeasure", "PrescClass", "ColdChainFlag",
            "ShelfLifeMonths", "ShelfLifeGroup", "ProductStatus"]
    scd2 = ["StandardCost", "ListTradePrice", "MRP"]
    cols = ["GTIN14", "InternalSKU"] + scd1 + scd2
    inserted = versions = 0
    first_load = count(con, "DimMedicine") == 0
    con.register("src_m", med)
    if not first_load:
        sets = ", ".join(f"{c} = s.{c}" for c in scd1)
        diff = " OR ".join(f"DimMedicine.{c} IS DISTINCT FROM s.{c}" for c in scd1)
        con.execute(f"UPDATE DimMedicine SET {sets} FROM src_m s "      # SCD1 on every version
                    f"WHERE DimMedicine.InternalSKU = s.InternalSKU AND ({diff})")
        pdiff = " OR ".join(f"d.{c} IS DISTINCT FROM CAST(s.{c} AS DECIMAL(10,2))" for c in scd2)
        changed = con.execute(f"""SELECT d.MedKey, s.InternalSKU FROM DimMedicine d
                                  JOIN src_m s ON d.InternalSKU = s.InternalSKU
                                  WHERE d.IsCurrent AND ({pdiff})""").fetchall()
        if changed:                                                       # SCD2: close old, add new
            keys = [k for k, _ in changed]
            too_early = con.execute(f"SELECT COUNT(*) FROM DimMedicine WHERE MedKey IN ({','.join(map(str, keys))}) "
                                    f"AND ValidFrom >= DATE '{effective}'").fetchall()[0][0]
            if too_early:
                raise BuildError(f"SCD2: price changed but effective date {effective} is not after the current "
                                 f"version start. Run with --effective-date YYYY-MM-DD.")
            con.execute(f"UPDATE DimMedicine SET ValidTo = DATE '{effective - timedelta(days=1)}', "
                        f"IsCurrent = FALSE WHERE MedKey IN ({','.join(map(str, keys))})")
            newv = med[med.InternalSKU.isin([s for _, s in changed])].copy()
            start = next_key(con, "DimMedicine", "MedKey")
            newv.insert(0, "MedKey", range(start, start + len(newv)))
            newv["ValidFrom"], newv["ValidTo"], newv["IsCurrent"] = effective, None, True
            versions = insert_df(con, "DimMedicine", newv)
            log.info("DimMedicine SCD2: %d price change(s) -> new versions valid from %s", versions, effective)
    con.unregister("src_m")
    have = {r[0] for r in con.execute("SELECT InternalSKU FROM DimMedicine").fetchall()}
    new = med[~med.InternalSKU.isin(have)].copy()
    start = next_key(con, "DimMedicine", "MedKey")
    new.insert(0, "MedKey", range(start, start + len(new)))
    new["ValidFrom"] = FIRST_VALID_FROM if first_load else effective
    new["ValidTo"], new["IsCurrent"] = None, True
    inserted = insert_df(con, "DimMedicine", new)
    return inserted + versions


def load_dim_pharmacy(con, sales):
    g = sales.groupby("PharmacyID", sort=True)
    for c in ["PharmacyGLN", "PharmacyName", "PharmacyType", "HospitalID"]:
        varying = (g[c].nunique(dropna=False) > 1).sum()
        if varying:
            log.warning("DimPharmacy: %s changes inside %d pharmacies; first value used", c, varying)
    p = g[["PharmacyGLN", "PharmacyName", "PharmacyType", "HospitalID"]].first().reset_index()
    hk = dict(con.execute("SELECT HospitalID, HospitalKey FROM DimHospital").fetchall())
    blank = p["HospitalID"].isna()                                   # blank -> row 0 'No hospital'
    unknown = sorted(set(p.loc[~blank, "HospitalID"]) - set(hk))
    if unknown:
        raise BuildError(f"DimPharmacy: HospitalID {unknown} not found in DimHospital.")
    p["HospitalKey"] = p["HospitalID"].map(hk).where(~blank, NO_HOSPITAL_KEY).astype("int64")
    p = p.drop(columns="HospitalID")
    dup = p[p.duplicated("PharmacyName", keep=False)]
    if not dup.empty:
        raise BuildError(f"DimPharmacy: abbreviated names are not unique: {dup.PharmacyName.tolist()[:6]}")
    con.register("src_p", p)
    con.execute("""UPDATE DimPharmacy SET PharmacyGLN = s.PharmacyGLN, PharmacyName = s.PharmacyName,
                          PharmacyType = s.PharmacyType, HospitalKey = s.HospitalKey
                   FROM src_p s WHERE DimPharmacy.PharmacyID = s.PharmacyID AND
                        (DimPharmacy.PharmacyGLN IS DISTINCT FROM s.PharmacyGLN OR
                         DimPharmacy.PharmacyName IS DISTINCT FROM s.PharmacyName OR
                         DimPharmacy.PharmacyType IS DISTINCT FROM s.PharmacyType OR
                         DimPharmacy.HospitalKey IS DISTINCT FROM s.HospitalKey)""")
    con.unregister("src_p")
    have = {r[0] for r in con.execute("SELECT PharmacyID FROM DimPharmacy").fetchall()}
    new = p[~p.PharmacyID.isin(have)].copy()
    start = next_key(con, "DimPharmacy", "PharmacyKey")
    new.insert(0, "PharmacyKey", range(start, start + len(new)))
    return insert_df(con, "DimPharmacy", new)


def load_dim_location(con, sales, demand):
    pairs = pd.concat([sales[["State", "City"]], demand[["State", "City"]]]).drop_duplicates()
    pairs = pairs.sort_values(["State", "City"]).reset_index(drop=True)
    bad = pairs[pairs.State.str.len() != 2]
    if not bad.empty:
        log.warning("DimLocation: State is not a 2-letter code for %s", bad.State.unique().tolist()[:5])
    have = set(con.execute("SELECT State, City FROM DimLocation").fetchall())
    new = pairs[[(s, c) not in have for s, c in zip(pairs.State, pairs.City)]].copy()
    start = next_key(con, "DimLocation", "LocationKey")
    new.insert(0, "LocationKey", range(start, start + len(new)))
    new.insert(1, "Country", "US")
    return insert_df(con, "DimLocation", new[["LocationKey", "Country", "State", "City"]])


def load_dim_sales_profile(con, sales):
    cols = ["ChannelGroup", "SalesChannel", "CustomerSegment", "DeliveryOTIFFlag"]
    combos = sales[cols].drop_duplicates().sort_values(cols).reset_index(drop=True)
    have = set(con.execute(f"SELECT {', '.join(cols)} FROM DimSalesProfile").fetchall())
    new = combos[[tuple(r) not in have for r in combos.itertuples(index=False)]].copy()
    start = next_key(con, "DimSalesProfile", "SalesProfileKey")
    new.insert(0, "SalesProfileKey", range(start, start + len(new)))
    return insert_df(con, "DimSalesProfile", new)


# load facts (surrogate-key lookup, no silent row loss)

def fail_on_missing_keys(con, tmp, key_cols, label):
    for k in key_cols:
        n = con.execute(f"SELECT COUNT(*) FROM {tmp} WHERE {k} IS NULL").fetchall()[0][0]
        if n:
            raise BuildError(f"{label}: {n} rows found no {k} (the lookup in the dimension failed).")


def load_fact_sales(con, sales):
    have = {r[0] for r in con.execute("SELECT SalesTransactionID FROM FactSales").fetchall()}
    new = sales[~sales.SalesTransactionID.isin(have)].copy()
    if new.empty:
        return 0
    new["DateKey"] = new.SalesDate.dt.year * 10000 + new.SalesDate.dt.month * 100 + new.SalesDate.dt.day
    con.register("new_sales", new)
    con.execute("""CREATE OR REPLACE TEMP TABLE keyed_sales AS
        SELECT s.DateKey, m.MedKey, p.PharmacyKey, l.LocationKey, sp.SalesProfileKey,
               s.SalesTransactionID, s.QuantitySoldPacks, s.UnitRetailPriceUSD, s.GrossSalesAmountUSD,
               s.DiscountAmountUSD, s.NetSalesAmountUSD, s.SalesTaxUSD, s.CostOfGoodsSoldUSD,
               s.GrossMarginUSD, s.DaysToExpiryAtSale
        FROM new_sales s
        LEFT JOIN DimMedicine m ON m.GTIN14 = s.GTIN14                       -- SCD2: version valid on the sale date
             AND CAST(s.SalesDate AS DATE) >= m.ValidFrom
             AND CAST(s.SalesDate AS DATE) <= COALESCE(m.ValidTo, DATE '9999-12-31')
        LEFT JOIN DimPharmacy p ON p.PharmacyID = s.PharmacyID
        LEFT JOIN DimLocation l ON l.State = s.State AND l.City = s.City
        LEFT JOIN DimSalesProfile sp ON sp.ChannelGroup = s.ChannelGroup AND sp.SalesChannel = s.SalesChannel
             AND sp.CustomerSegment = s.CustomerSegment AND sp.DeliveryOTIFFlag = s.DeliveryOTIFFlag""")
    con.unregister("new_sales")
    fail_on_missing_keys(con, "keyed_sales", ["MedKey", "PharmacyKey", "LocationKey", "SalesProfileKey"], "FactSales")
    n = count(con, "keyed_sales")
    if n != len(new):
        raise BuildError(f"FactSales: {len(new)} new source rows but {n} after key lookup (a join duplicated rows).")
    con.execute("INSERT INTO FactSales SELECT * FROM keyed_sales")
    return n


def load_fact_demand(con, demand):
    have = {r[0] for r in con.execute("SELECT DemandRecordID FROM FactDemand").fetchall()}
    new = demand[~demand.DemandRecordID.isin(have)].copy()
    if new.empty:
        return 0
    new["DateKey"] = new.DemandMonth.dt.year * 10000 + new.DemandMonth.dt.month * 100 + new.DemandMonth.dt.day
    con.register("new_demand", new)
    con.execute("""CREATE OR REPLACE TEMP TABLE keyed_demand AS
        SELECT d.DateKey, m.MedKey, h.HospitalKey, l.LocationKey, d.DemandRecordID,
               d.EstimatedDiseaseCases, d.DiseaseRelatedAdmissions, d.OutpatientEncounters,
               d.ForecastDemandPacks, d.RequestedQuantityPacks, d.ApprovedQuantityPacks,
               d.ReceivedQuantityPacks, d.EmergencyRequestQuantityPacks, d.OpeningStockPacks,
               d.DispensedQuantityPacks, d.ConsumedQuantityPacks, d.WastageQuantityPacks,
               d.ClosingStockPacks, d.UnfulfilledDemandPacks, d.DemandTrendIndex
        FROM new_demand d
        LEFT JOIN DimMedicine m ON m.GTIN14 = d.GTIN14
             AND CAST(d.DemandMonth AS DATE) >= m.ValidFrom
             AND CAST(d.DemandMonth AS DATE) <= COALESCE(m.ValidTo, DATE '9999-12-31')
        LEFT JOIN DimHospital h ON h.HospitalID = d.HospitalID
        LEFT JOIN DimLocation l ON l.State = d.State AND l.City = d.City""")
    con.unregister("new_demand")
    fail_on_missing_keys(con, "keyed_demand", ["MedKey", "HospitalKey", "LocationKey"], "FactDemand")
    n = count(con, "keyed_demand")
    if n != len(new):
        raise BuildError(f"FactDemand: {len(new)} new source rows but {n} after key lookup (a join duplicated rows).")
    con.execute("INSERT INTO FactDemand SELECT * FROM keyed_demand")
    return n


#checks inside the transaction (fail -> rollback)

def run_checks(con, first_load, sales_rows, demand_rows):
    problems = []
    fks = {
        "FactSales": [("DateKey", "DimDate"), ("MedKey", "DimMedicine"), ("PharmacyKey", "DimPharmacy"),
                      ("LocationKey", "DimLocation"), ("SalesProfileKey", "DimSalesProfile")],
        "FactDemand": [("DateKey", "DimDate"), ("MedKey", "DimMedicine"), ("HospitalKey", "DimHospital"),
                       ("LocationKey", "DimLocation")],
        "DimPharmacy": [("HospitalKey", "DimHospital")],
    }
    for table, refs in fks.items():
        for col, dim in refs:
            orphans = con.execute(f"SELECT COUNT(*) FROM {table} t LEFT JOIN {dim} d ON t.{col} = d.{col} "
                                  f"WHERE d.{col} IS NULL").fetchall()[0][0]
            if orphans:
                problems.append(f"{orphans} {table}.{col} values have no row in {dim}")
    multi = con.execute("""SELECT COUNT(*) FROM (SELECT InternalSKU FROM DimMedicine
                           GROUP BY InternalSKU HAVING SUM(CASE WHEN IsCurrent THEN 1 ELSE 0 END) <> 1)""").fetchall()[0][0]
    if multi:
        problems.append(f"{multi} medicines do not have exactly one current version")
    badrange = con.execute("SELECT COUNT(*) FROM DimMedicine WHERE ValidTo IS NOT NULL AND ValidTo < ValidFrom").fetchall()[0][0]
    if badrange:
        problems.append(f"{badrange} medicine versions end before they start")
    gaps = con.execute("SELECT COUNT(*) <> DATEDIFF('day', MIN(FullDate), MAX(FullDate)) + 1 FROM DimDate").fetchall()[0][0]
    if gaps:
        problems.append("DimDate has gaps")
    if count(con, "FactSales") < sales_rows or count(con, "FactDemand") < demand_rows:
        problems.append("a fact table has fewer rows than the staging data")
    if problems:
        raise BuildError("post-load checks failed:\n  - " + "\n  - ".join(problems))
    if first_load:
        for t, want in EXPECTED_FIRST_LOAD.items():
            got = count(con, t)
            (log.info if got == want else log.warning)("template check %-16s expected %9s  got %9s  %s",
                                                        t, f"{want:,}", f"{got:,}", "OK" if got == want else "<-- DIFFERENT")


def batch_start(con, sales, demand):
    """SCD2 date for a refresh = first date of the rows that are NEW in this batch (today if none)."""
    seen_s = {r[0] for r in con.execute("SELECT SalesTransactionID FROM FactSales").fetchall()}
    seen_d = {r[0] for r in con.execute("SELECT DemandRecordID FROM FactDemand").fetchall()}
    dates = [sales.loc[~sales.SalesTransactionID.isin(seen_s), "SalesDate"],
             demand.loc[~demand.DemandRecordID.isin(seen_d), "DemandMonth"]]
    dates = [d.min().date() for d in dates if not d.empty]
    return min(dates) if dates else date.today()

# MAIN
def build(sources, db_path=WAREHOUSE, full_rebuild=False, effective=None):
    """Whole run. `sources` = dict of raw DataFrames {'master','sales','demand'}."""
    check_schemas(sources)
    med = prep_medicine(sources.pop("master"))
    sales = prep_sales(sources.pop("sales"), med)
    demand = prep_demand(sources.pop("demand"))
    gc.collect()
    con = duckdb.connect(db_path)
    try:
        con.execute("SET memory_limit = '512MB'")
        con.execute("BEGIN")
        if full_rebuild:
            for t in reversed(TABLE_ORDER):
                con.execute(f"DROP TABLE IF EXISTS {t}")
        for t in TABLE_ORDER:
            con.execute(DDL[t])
        first_load = count(con, "FactSales") == 0 and count(con, "DimMedicine") == 0
        if first_load:
            effective = FIRST_VALID_FROM
        elif effective is None:
            effective = batch_start(con, sales, demand)
        before = {t: count(con, t) for t in TABLE_ORDER}

        load_dim_date(con, sales, demand)
        load_dim_hospital(con, demand)
        load_dim_medicine(con, med, effective)
        load_dim_pharmacy(con, sales)
        load_dim_location(con, sales, demand)
        load_dim_sales_profile(con, sales)
        load_fact_sales(con, sales)
        load_fact_demand(con, demand)

        run_checks(con, first_load, len(sales), len(demand))
        after = {t: count(con, t) for t in TABLE_ORDER}
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()

    log.info("%-16s %10s %10s %10s", "table", "before", "after", "added")
    for t in TABLE_ORDER:
        log.info("%-16s %10s %10s %10s", t, f"{before[t]:,}", f"{after[t]:,}", f"{after[t] - before[t]:+,}")
    return before, after

def main(argv=None):
    ap = argparse.ArgumentParser(description="Build / refresh the SDI star schema in DuckDB")
    ap.add_argument("--db", default=WAREHOUSE)
    ap.add_argument("--full-rebuild", action="store_true", help="drop all star tables first")
    ap.add_argument("--effective-date", type=date.fromisoformat, help="SCD2 date for price changes (YYYY-MM-DD)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", stream=sys.stdout)
    try:
        build(read_sources(), args.db, args.full_rebuild, args.effective_date)
    except BuildError as e:
        log.error("BUILD FAILED, warehouse left unchanged:\n%s", e)
        return 1
    log.info("star schema OK -> %s", args.db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
