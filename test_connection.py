import duckdb, psycopg2
psycopg2.connect(host="localhost", port=5433, dbname="staging", user="dwh_user", password="dwh_pass")
print("postgres ok")
duckdb.connect("warehouse.duckdb")
print("duckdb ok", duckdb.__version__)