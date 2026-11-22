#!/usr/bin/env python3
"""
load_staging.py  -  CSV files  ->  Postgres staging tables (stg_master, stg_sales, stg_demand)

* Every column is loaded as TEXT, exactly as it is in the file (no value is changed or lost,
  leading zeros of GTIN14/GLN stay). Typing and cleaning happen later in build_star_schema.py.
* Uses Postgres COPY, so the 218 MB sales file loads in seconds, not minutes.
* Every table load writes one row to load_metrics (batch, rows, seconds).

  python load_staging.py                                   # first load, replaces the 3 tables
  python load_staging.py --mode append --batch-id refresh1 --data-dir ..\\data\\refresh1
"""
import argparse
import csv
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2

PG = dict(host=os.getenv("PGHOST", "localhost"), port=int(os.getenv("PGPORT", "5433")),
          user=os.getenv("PGUSER", "postgres"), password=os.getenv("PGPASSWORD", "postgres"),
          dbname=os.getenv("PGDATABASE", "postgres"))
SCHEMA = os.getenv("PG_SCHEMA", "public")
DATA_DIR = Path(os.getenv("DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
FILES = [  # (file name, staging table)
    ("01_master_data.csv", os.getenv("STG_MASTER", "stg_master")),
    ("05_pharmacy_sales_data.csv", os.getenv("STG_SALES", "stg_sales")),
    ("06_hospital_demand_data.csv", os.getenv("STG_DEMAND", "stg_demand")),
]
METRICS_COLS = ["batch_id", "table_name", "source_file", "mode", "rows_loaded", "table_rows_after",
                "seconds", "loaded_at"]
METRICS_DDL = """CREATE TABLE load_metrics (batch_id TEXT, table_name TEXT, source_file TEXT, mode TEXT,
                 rows_loaded BIGINT, table_rows_after BIGINT, seconds NUMERIC(10,2),
                 loaded_at TIMESTAMP DEFAULT now())"""


def q(name):
    return '"' + name.replace('"', '""') + '"'


def ensure_metrics_table(cur):
    """Create load_metrics. An old table with other columns is replaced (it is only a log)."""
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name='load_metrics'",
                (SCHEMA,))
    have = [r[0] for r in cur.fetchall()]
    if have and have != METRICS_COLS:
        cur.execute("DROP TABLE load_metrics")
        have = []
    if not have:
        cur.execute(METRICS_DDL)


def load_one(cur, path, table, mode, batch_id):
    with open(path, encoding="utf-8-sig", newline="") as f:
        header = next(csv.reader(f))
    cols = [h.strip() for h in header]
    full = f"{q(SCHEMA)}.{q(table)}"
    if mode == "append":
        cur.execute("SELECT 1 FROM load_metrics WHERE batch_id=%s AND table_name=%s", (batch_id, table))
        if cur.fetchone():
            print(f"{table}: batch '{batch_id}' already loaded, skipped")
            return
    t0 = time.time()
    if mode == "replace":
        cur.execute(f"DROP TABLE IF EXISTS {full} CASCADE")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {full} ({', '.join(q(c) + ' TEXT' for c in cols)})")
    collist = ", ".join(q(c) for c in cols)
    with open(path, "rb") as f:
        cur.copy_expert(f"COPY {full} ({collist}) FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')", f)
    loaded = cur.rowcount
    cur.execute(f"SELECT COUNT(*) FROM {full}")
    after = cur.fetchone()[0]
    secs = round(time.time() - t0, 2)
    cur.execute("INSERT INTO load_metrics (batch_id, table_name, source_file, mode, rows_loaded, table_rows_after, seconds) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)", (batch_id, table, path.name, mode, loaded, after, secs))
    print(f"{table:12s} {path.name:30s} loaded {loaded:>9,} rows, table now {after:>9,}  ({secs}s)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["replace", "append"], default="replace")
    ap.add_argument("--batch-id", default=None, help="name of this load (default: initial / timestamp)")
    ap.add_argument("--data-dir", type=Path, default=DATA_DIR)
    a = ap.parse_args()
    batch = a.batch_id or ("initial" if a.mode == "replace" else datetime.now().strftime("batch_%Y%m%d_%H%M%S"))
    missing = [f for f, _ in FILES if not (a.data_dir / f).exists()]
    if missing:
        print(f"ERROR: missing files in {a.data_dir}: {missing}")
        return 1
    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:          # one transaction: all 3 tables load or none
            cur.execute(f"SET search_path TO {q(SCHEMA)}")
            ensure_metrics_table(cur)
            for fname, table in FILES:
                load_one(cur, a.data_dir / fname, table, a.mode, batch)
    finally:
        conn.close()
    print(f"staging load OK (batch '{batch}', mode {a.mode})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

# --- GTIN14 fix: CSV has scientific notation, restore from MedicineID join ---
def fix_gtin14():
    import psycopg2, os
    c = psycopg2.connect(host=os.getenv('PGHOST','localhost'), port=int(os.getenv('PGPORT','5433')),
                         user=os.getenv('PGUSER','postgres'), password=os.getenv('PGPASSWORD','postgres'),
                         dbname=os.getenv('PGDATABASE','postgres'))
    cur = c.cursor()
    cur.execute('UPDATE stg_sales s SET "GTIN14" = m."GTIN14" FROM stg_master m WHERE CAST(s."MedicineID" AS TEXT) = CAST(m."MedicineID" AS TEXT)')
    cur.execute('UPDATE stg_demand d SET "GTIN14" = m."GTIN14" FROM stg_master m WHERE CAST(d."MedicineID" AS TEXT) = CAST(m."MedicineID" AS TEXT)')
    c.commit()
    print("GTIN14 patched via MedicineID join")

fix_gtin14()
