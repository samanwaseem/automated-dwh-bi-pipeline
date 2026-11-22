import subprocess, duckdb, os
result = subprocess.run(['find', '/opt/airflow', '-name', '*.duckdb', '-type', 'f'], capture_output=True, text=True)
print("DuckDB files found:")
print(result.stdout)
for f in result.stdout.strip().split('\n'):
    if f:
        try:
            c = duckdb.connect(f, read_only=True)
            tables = c.sql('SHOW TABLES').fetchall()
            print(f'{f}: {[t[0] for t in tables]}')
            c.close()
        except Exception as e:
            print(f'{f}: error - {e}')
