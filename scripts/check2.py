import psycopg2
c = psycopg2.connect(host='staging-db', port=5432, user='dwh_user', password='dwh_pass', dbname='staging')
cur = c.cursor()
cur.execute("SELECT DISTINCT \"GTIN14\" FROM stg_master LIMIT 5")
print("MASTER GTIN14:", [r[0] for r in cur.fetchall()])
cur.execute("SELECT DISTINCT \"GTIN14\" FROM stg_sales LIMIT 5")
print("SALES GTIN14:", [r[0] for r in cur.fetchall()])
cur.execute("SELECT COUNT(*) FROM stg_sales s JOIN stg_master m ON s.\"GTIN14\" = m.\"GTIN14\"")
print("JOIN COUNT:", cur.fetchone()[0])
