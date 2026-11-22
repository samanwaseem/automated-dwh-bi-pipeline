import psycopg2
c = psycopg2.connect(host='staging-db', port=5432, user='dwh_user', password='dwh_pass', dbname='staging')
cur = c.cursor()
for t in ['stg_master', 'stg_sales', 'stg_demand']:
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s ORDER BY ordinal_position", (t,))
    print(t, [r[0] for r in cur.fetchall()])
