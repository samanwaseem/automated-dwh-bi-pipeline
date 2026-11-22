import duckdb
c = duckdb.connect('/opt/airflow/project/scripts/warehouse.duckdb', read_only=True)
for t in ['DimDate','DimMedicine','DimPharmacy','DimHospital','DimLocation','DimSalesProfile','FactSales','FactDemand']:
    count = c.sql(f'SELECT COUNT(*) FROM {t}').fetchall()[0][0]
    print(f'{t}: {count:,} rows')
c.close()
