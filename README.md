# Pharma Cold Chain DWH BI 
End-to-end automated data warehousing and BI pipeline for pharmaceutical supply chain intelligence, orchestrated with Apache Airflow.

## Group Members
- Saman Waseem 29258
- Zara Asim 29262

## Video Demo
https://drive.google.com/drive/folders/1UyIjCLCTfldWQToPTt-oCqf0j8ERv4tm

## How To Run

### Prerequisites
* **Docker Desktop** installed and running (minimum 4 GB RAM and 5 GB free disk space).
* **Python 3.10+** (used to generate the Airflow Fernet key).
* Clone or extract this repository into a **local folder** (avoid OneDrive, Google Drive, or iCloud folders to prevent Docker file-locking issues).

---

### Step 1: Clone the Repository or Download as a ZIP File and Extract to Folder
```bash
git clone <https://github.com/samanwaseem/automated-dwh-bi-pipeline>
cd automated-dwh-bi-pipeline
```

### Step 2: Add the Raw CSV Files
Create a `data/` folder in the repository root and place the 3 required CSV files inside it:
```text
automated-dwh-bi-pipeline/
└── data/
    ├── 01_master_data.csv
    ├── 05_pharmacy_sales_data.csv
    └── 06_hospital_demand_data.csv
```

### Step 3: Create `airflow/.env`

**Windows (PowerShell):**
```powershell
cd airflow
$fernet = python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
@"
AIRFLOW_UID=50000
FERNET_KEY=$fernet
"@ | Set-Content -Path .env -Encoding utf8
```

**macOS / Linux (Bash):**
```bash
cd airflow
FERNET_KEY=$(python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
printf "AIRFLOW_UID=50000\nFERNET_KEY=%s\n" "$FERNET_KEY" > .env
```

### Step 4: Start the Docker Stack
From inside the `airflow/` directory:
```bash
docker compose up -d
```
Wait 1–2 minutes for initialization to finish, then confirm all containers show `Up` / `Healthy`:
```bash
docker ps
```

### Step 5: Log In to Airflow
1. Open **`http://localhost:8081`** in your browser. 
2. Log in with the default credentials:
   * **Username:** `airflow`
   * **Password:** `airflow`

### Step 6: Run the Initial Load
1. On the **DAGs** page, unpause **`pharma_sdi_pipeline`**.
2. Click **Trigger DAG** (play button) and enter these **Run Parameters**:
   * **Mode:** `replace`
   * **Batch name:** `initial`
   * **Folder:** `/opt/airflow/project/data`
3. Click **Trigger** and wait for all tasks in the **Grid** view to turn **dark green**.

### Step 7: Run Incremental Refreshes (`refresh1`, `refresh2`)
1. Place each refresh batch's 3 CSV files in its subfolder under `data/` (or generate them via `scripts/generate_refresh_batches.py`).
2. Click **Trigger DAG** on **`pharma_sdi_pipeline`** and set:
   * **Mode:** `append`
   * **Batch name:** `refresh1` *(then `refresh2` on the next run)*
   * **Folder:** `/opt/airflow/project/data/<refresh_folder>`
3. Click **Trigger** and wait for all tasks to turn **dark green**.

### Step 8: Verify the Output
1. In the Airflow UI, click **`validate_staging`** → **Logs** and confirm **`0 FAIL`**.
2. Confirm `scripts/warehouse.duckdb` was generated. To inspect table row counts locally:
```bash
python -c "import duckdb; con=duckdb.connect('scripts/warehouse.duckdb', read_only=True); [print(t[0], con.execute(f'SELECT COUNT(*) FROM {t[0]}').fetchone()[0]) for t in con.execute('SHOW TABLES').fetchall()]"
```

### Step 9: Stop the Stack When Finished
From inside the `airflow/` directory:
```bash
# Stop containers (keeps database data)
docker compose down

# Stop containers and wipe all volumes for a clean reset
docker compose down -v
```
