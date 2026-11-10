import duckdb, time, statistics

c = duckdb.connect('warehouse.duckdb', read_only=True)
for f in ['q1_forecast_vs_actual.sql', 'q2_demand_trend.sql', 'q3_pharmacy_sales.sql']:
    sql = open(f).read()
    runs = []
    for _ in range(5):
        t = time.time()
        c.sql(sql).fetchall()
        runs.append(time.time() - t)
    print(f, 'median seconds:', round(statistics.median(runs), 3))