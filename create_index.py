import duckdb

c = duckdb.connect('warehouse.duckdb')
c.sql('DROP INDEX IF EXISTS idx_sales_pharmacy')
c.sql('CREATE INDEX idx_sales_pharmacy ON fact_sales(pharmacy_key)')
print('index created')