import os
from pathlib import Path
import duckdb

DB = os.getenv("WAREHOUSE_DB", str(Path(__file__).with_name("warehouse.duckdb")))
c = duckdb.connect(DB)
c.execute("DROP INDEX IF EXISTS idx_sales_pharmacy")
c.execute("CREATE INDEX idx_sales_pharmacy ON FactSales(PharmacyKey)")
print("index created: idx_sales_pharmacy on FactSales(PharmacyKey) in", DB)
