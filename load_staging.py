"""Step 1: load the three raw CSVs into Postgres staging (everything as TEXT, nothing changed).
Run from the IDA PROJECT folder:  python load_staging.py
"""
import csv, time, datetime, os
import psycopg2

DATA_DIR = "data"
FILES = {  # staging table : (csv file name, expected row count)
    "stg_master":   ("01_master_data.csv",         63),
    "stg_sales":    ("05_pharmacy_sales_data.csv", 500000),
    "stg_demand":   ("06_hospital_demand_data.csv", 27216),
}
DB = dict(host="localhost", port=5433, dbname="staging", user="dwh_user", password="dwh_pass")


def count_rows(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return sum(1 for _ in csv.reader(f)) - 1  # minus header


def load_one(cur, table, path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        header = next(csv.reader(f))
    cols = ", ".join(f'"{c}" TEXT' for c in header)
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.execute(f"CREATE TABLE {table} ({cols})")
    with open(path, newline="", encoding="utf-8-sig") as f:
        cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)", f)
    return cur.rowcount


def main():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS load_metrics (
        id SERIAL PRIMARY KEY, file_name TEXT, table_name TEXT, started_at TIMESTAMP, ended_at TIMESTAMP,
        duration_sec NUMERIC, rows_in_file INT, rows_loaded INT, status TEXT, error TEXT)""")
    conn.commit()
    for table, (fname, expected) in FILES.items():
        path = os.path.join(DATA_DIR, fname)
        start = datetime.datetime.now(); t0 = time.time()
        rows_in_file = rows_loaded = None; status, error = "success", None
        try:
            rows_in_file = count_rows(path)
            rows_loaded = load_one(cur, table, path)
            if rows_loaded != rows_in_file:
                raise ValueError(f"loaded {rows_loaded} rows but file has {rows_in_file}")
            conn.commit()
        except Exception as e:
            conn.rollback(); status, error = "failed", str(e)
        dur = round(time.time() - t0, 2)
        cur.execute("""INSERT INTO load_metrics (file_name, table_name, started_at, ended_at, duration_sec,
                       rows_in_file, rows_loaded, status, error) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (fname, table, start, datetime.datetime.now(), dur, rows_in_file, rows_loaded, status, error))
        conn.commit()
        note = "" if rows_loaded == expected else f"  <-- expected {expected}"
        print(f"{fname}: {status}, {rows_loaded} rows, {dur}s{note}" + (f"  ERROR: {error}" if error else ""))
    # sanity check: the master GTIN14 column must not be damaged
    cur.execute('SELECT COUNT(DISTINCT "GTIN14") FROM stg_master')
    n = cur.fetchone()[0]
    print("GTIN14 distinct values in master:", n, "(should be 63)" if n != 63 else "(ok)")
    conn.close()


if __name__ == "__main__":
    main()