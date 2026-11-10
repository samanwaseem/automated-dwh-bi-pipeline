"""Step 2: validate the staging tables (rules from the project spec: price > 0, valid dates, non-null IDs).
Bad rows are NOT deleted. Their IDs are saved in rejected_rows, and counts go to validation_results.
Run from the IDA PROJECT folder after load_staging.py:  python validate_staging.py
"""
import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from psycopg2.extras import execute_values

DB = dict(host="localhost", port=5433, dbname="staging", user="dwh_user", password="dwh_pass")

# table: (id column, [(rule name, column, kind, date format)])
RULES = {
    "stg_master": ("MedicineID", [
        ("MedicineID not null", "MedicineID", "notnull", None),
        ("InternalSKU not null", "InternalSKU", "notnull", None),
        ("EffectiveFromDate valid date", "EffectiveFromDate", "date", ("%m/%d/%Y", "%Y-%m-%d")),
        ("UnitManufacturingCostUSD > 0", "UnitManufacturingCostUSD", "positive", None),
        ("WholesalePriceUSD > 0", "WholesalePriceUSD", "positive", None),
        ("RecommendedRetailPriceUSD > 0", "RecommendedRetailPriceUSD", "positive", None)]),
    "stg_sales": ("SalesTransactionID", [
        ("SalesTransactionID not null", "SalesTransactionID", "notnull", None),
        ("MedicineID not null", "MedicineID", "notnull", None),
        ("SalesDate valid date", "SalesDate", "date", ("%Y-%m-%d", "%m/%d/%Y")),
        ("UnitRetailPriceUSD > 0", "UnitRetailPriceUSD", "positive", None)]),
    "stg_demand": ("HospitalDemandRecordID", [
        ("HospitalDemandRecordID not null", "HospitalDemandRecordID", "notnull", None),
        ("HospitalID not null", "HospitalID", "notnull", None),
        ("MedicineID not null", "MedicineID", "notnull", None),
        ("DemandMonth valid date", "DemandMonth", "date", ("%m/%d/%Y", "%Y-%m-%d"))]),
}


def failed_mask(series, kind, fmt):
    s = series.astype("string")
    if kind == "notnull":
        return s.isna() | (s.str.strip() == "")
    if kind == "date":
        ok = pd.Series(False, index=s.index)
        for f in ([fmt] if isinstance(fmt, str) else fmt):   # a date is fine if it matches any allowed format
            ok = ok | pd.to_datetime(s, format=f, errors="coerce").notna().fillna(False)
        return ~ok
    if kind == "positive":
        ok = (pd.to_numeric(s, errors="coerce") > 0).fillna(False)  # missing or non-numeric counts as bad
        return ~ok


def main():
    conn = psycopg2.connect(**DB); cur = conn.cursor()
    engine = create_engine("postgresql+psycopg2://dwh_user:dwh_pass@localhost:5433/staging")
    cur.execute("""CREATE TABLE IF NOT EXISTS validation_results (
        run_at TIMESTAMP, table_name TEXT, rule TEXT, rows_checked INT, rows_failed INT)""")
    cur.execute("DROP TABLE IF EXISTS rejected_rows")
    cur.execute("CREATE TABLE rejected_rows (table_name TEXT, record_id TEXT, rule TEXT)")
    conn.commit()
    run_at = datetime.datetime.now()
    for table, (id_col, rules) in RULES.items():
        cols = sorted({id_col} | {r[1] for r in rules})
        df = pd.read_sql(f"SELECT {', '.join(chr(34) + c + chr(34) for c in cols)} FROM {table}", engine)
        print(f"\n{table}: {len(df)} rows")
        for rule, col, kind, fmt in rules:
            bad = failed_mask(df[col], kind, fmt).fillna(True).astype(bool)
            n_bad = int(bad.sum())
            cur.execute("INSERT INTO validation_results VALUES (%s,%s,%s,%s,%s)", (run_at, table, rule, len(df), n_bad))
            if n_bad:
                execute_values(cur, "INSERT INTO rejected_rows VALUES %s",
                               [(table, str(i), rule) for i in df.loc[bad, id_col].tolist()])
            print(f"  {'PASS' if n_bad == 0 else 'FAIL'}  {rule}: {n_bad} bad rows")
        conn.commit()
    cur.execute("SELECT COUNT(*) FROM rejected_rows")
    print("\nTotal rejected row entries saved in rejected_rows:", cur.fetchone()[0])
    conn.close()


if __name__ == "__main__":
    main()