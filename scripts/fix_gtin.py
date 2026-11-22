import psycopg2, os
c = psycopg2.connect(host=os.getenv('PGHOST','staging-db'), port=int(os.getenv('PGPORT','5432')),
    user=os.getenv('PGUSER','dwh_user'), password=os.getenv('PGPASSWORD','dwh_pass'),
    dbname=os.getenv('PGDATABASE','staging'))
cur = c.cursor()
cur.execute('UPDATE stg_sales s SET "GTIN14" = m."GTIN14" FROM stg_master m WHERE CAST(s."MedicineID" AS TEXT) = CAST(m."MedicineID" AS TEXT)')
cur.execute('UPDATE stg_demand d SET "GTIN14" = m."GTIN14" FROM stg_master m WHERE CAST(d."MedicineID" AS TEXT) = CAST(m."MedicineID" AS TEXT)')
c.commit()
print("GTIN14 patched via MedicineID join")
