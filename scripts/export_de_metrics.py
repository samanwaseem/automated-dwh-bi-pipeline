#!/usr/bin/env python3
"""
export_de_metrics.py - Exports DE Observability Metrics to de_observability_metrics.xlsx:
  1. Batch_Ingestion  (from Postgres load_metrics)
  2. Storage_Staging  (Postgres table sizes & row counts + DuckDB star schema counts & file size)
  3. Querying         (DuckDB execution times, row counts, and status for Q1, Q2, Q3)
"""
import os
import time
from pathlib import Path
import duckdb
import pandas as pd
import psycopg2

SCRIPTS_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPTS_DIR.parent
DUCKDB_PATH = SCRIPTS_DIR / "warehouse.duckdb"
OUT_XLSX = ROOT_DIR / "de_observability_metrics.xlsx"

PG_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5433")),
    "dbname": os.getenv("PGDATABASE", "staging"),
    "user": os.getenv("PGUSER", "dwh_user"),
    "password": os.getenv("PGPASSWORD", "dwh_pass"),
}

PG = dict(host=os.getenv("PGHOST", "localhost"), port=int(os.getenv("PGPORT", "5433")),
          user=os.getenv("PGUSER", "dwh_user"), password=os.getenv("PGPASSWORD", "dwh_pass"),
          dbname=os.getenv("PGDATABASE", "staging"))

SQL_FILES = [
    "q1_forecast_vs_actual.sql",
    "q2_demand_trend.sql",
    "q3_pharmacy_sales.sql",
]


def get_batch_ingestion(pg_conn) -> pd.DataFrame:
    try:
        df = pd.read_sql("SELECT * FROM load_metrics ORDER BY 1;", pg_conn)
        # Remove timezone info so Excel doesn't complain
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_localize(None)
        return df
    except Exception as e:
        print(f"Warning: Could not read load_metrics: {e}")
        return pd.DataFrame([{"error": str(e)}])


def get_storage_metrics(pg_conn, duck_conn) -> pd.DataFrame:
    rows = []

    # 1. Postgres staging tables
    pg_sql = """
        SELECT
            relname AS table_name,
            n_live_tup AS approx_rows,
            ROUND(pg_total_relation_size(relid) / 1048576.0, 2) AS size_mb
        FROM pg_stat_user_tables
        ORDER BY relname;
    """
    pg_df = pd.read_sql(pg_sql, pg_conn)
    for _, r in pg_df.iterrows():
        tname = r["table_name"]
        exact_cnt = pd.read_sql(f'SELECT COUNT(*) AS c FROM "{tname}";', pg_conn)["c"].iloc[0]
        rows.append({
            "layer": "Postgres (Staging)",
            "table_name": tname,
            "row_count": int(exact_cnt),
            "size_mb": float(r["size_mb"]),
        })

    # 2. DuckDB star schema tables
    duck_size_mb = round(DUCKDB_PATH.stat().st_size / (1024 * 1024), 2) if DUCKDB_PATH.exists() else 0.0
    tables = [t[0] for t in duck_conn.execute("SHOW TABLES;").fetchall()]
    for tname in sorted(tables):
        cnt = duck_conn.execute(f'SELECT COUNT(*) FROM "{tname}";').fetchone()[0]
        rows.append({
            "layer": "DuckDB (Star Schema)",
            "table_name": tname,
            "row_count": int(cnt),
            "size_mb": duck_size_mb,  # Total warehouse.duckdb file size
        })

    return pd.DataFrame(rows)


def get_query_metrics(duck_conn, runs: int = 3) -> pd.DataFrame:
    results = []
    for fname in SQL_FILES:
        fpath = SCRIPTS_DIR / fname
        if not fpath.exists():
            results.append({"query": fname, "status": "MISSING", "median_ms": None, "avg_ms": None, "rows_returned": 0})
            continue

        sql = fpath.read_text(encoding="utf-8")
        times_ms = []
        row_count = 0
        status = "SUCCESS"
        error_msg = ""

        for _ in range(runs):
            t0 = time.perf_counter()
            try:
                res = duck_conn.execute(sql).fetchall()
                row_count = len(res)
                times_ms.append((time.perf_counter() - t0) * 1000.0)
            except Exception as e:
                status = "ERROR"
                error_msg = str(e)
                break

        results.append({
            "query": fname,
            "runs": len(times_ms),
            "median_ms": round(pd.Series(times_ms).median(), 2) if times_ms else None,
            "avg_ms": round(pd.Series(times_ms).mean(), 2) if times_ms else None,
            "rows_returned": row_count,
            "status": status,
            "error": error_msg,
        })

    return pd.DataFrame(results)


def main():
    print("Connecting to Postgres & DuckDB...")
    pg_conn = psycopg2.connect(**PG_CONFIG)
    duck_conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    try:
        df_batch = get_batch_ingestion(pg_conn)
        df_storage = get_storage_metrics(pg_conn, duck_conn)
        df_query = get_query_metrics(duck_conn, runs=3)

        with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
            df_batch.to_excel(writer, sheet_name="Batch_Ingestion", index=False)
            df_storage.to_excel(writer, sheet_name="Storage_Staging", index=False)
            df_query.to_excel(writer, sheet_name="Querying", index=False)

        print(f"\nSaved -> {OUT_XLSX}")
        print(f"  Batch_Ingestion : {len(df_batch)} rows")
        print(f"  Storage_Staging : {len(df_storage)} tables")
        print(f"  Querying        : {len(df_query)} queries")
    finally:
        pg_conn.close()
        duck_conn.close()


if __name__ == "__main__":
    main()
