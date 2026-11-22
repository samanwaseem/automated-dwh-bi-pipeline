import os, statistics, time
from pathlib import Path
import duckdb

HERE = Path(__file__).parent
DB = os.getenv("WAREHOUSE_DB", str(HERE / "warehouse.duckdb"))
c = duckdb.connect(DB, read_only=True)
for f in ["q1_forecast_vs_actual.sql", "q2_demand_trend.sql", "q3_pharmacy_sales.sql"]:
    sql = (HERE / f).read_text()
    runs = []
    for _ in range(5):
        t = time.time()
        rows = c.execute(sql).fetchall()
        runs.append(time.time() - t)
    print(f, "| rows:", len(rows), "| median seconds:", round(statistics.median(runs), 3))
