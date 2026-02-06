# Data Spectrum

Universal Metadata Extraction Tool for Databases, Data Lakes, and File Systems.

## 🏗 Architecture

Data Spectrum is designed as a modular, stateless service with a plugin architecture.

```mermaid
graph TD
    API[FastAPI Service] --> Reg[Source Registry]
    API --> Store[Metadata Store SQLite]
    API --> Factory[Connector Logic]
    
    Reg --> Enum[Encrypted Configs]
    
    Factory --> RDBMS[RDBMS Connector]
    Factory --> DBX[Databricks Connector]
    Factory --> File[File Connector]
    
    RDBMS --> SQL[SQLite / MySQL / Snowflake]
    DBX --> Cloud[Databricks SQL]
    File --> S3[S3 / GCS / Local]
    
    File --> Pq[Parquet Reader]
    File --> Delta[Delta Reader]
    File --> Ice[Iceberg Reader]
```

### Key Components
- **Core Interface**: Standardized `DataSource` API for all connectors.
- **Registry**: Secure storage for connection details (tokens/passwords encrypted with Fernet).
- **Persistence Layer**: Local SQLite cache with TTL (Time-To-Live) to prevent redundant scanning.
- **Unified File IO**: Uses `fsspec` to treat Local, S3, GCS, and ADLS paths uniformly.

## 🛠 Build & Test
Run the following script to execute integration tests and build the package:
```bash
./build.sh
```
This runs `pytest` (using `tests/integration`) and then `poetry build` ensuring only quality code is packaged.

## ✨ Features
- **Granular Extraction**: Get Table Schema, Column Stats (Min/Max/Nulls), and Partition Info.
- **Diverse Connectivity**:
    - **SQL**: SQLite, MySQL, Snowflake, PostgreSQL.
    - **Lakes**: Databricks (Hybrid SQL + File access).
    - **Files**: Recursive discovery for Parquet, Delta Lake, and Iceberg.
- **Scalable**:
    - Aggregates stats from thousands of file footers without reading full data.
    - Caches results to serve repeated requests instantly.

## � TPC-DS Data Generation (DuckDB)

Data Spectrum includes a utility to generate TPC-DS benchmark data in Parquet format and create a DuckDB database for easy testing.

**Generate Data:**
```bash
# Generate 0.01 GB data (very fast)
poetry run python utils/generate_tpcds.py --sf 0.01 --out data/tpcds --db tpcds.duckdb
```
This command:
1.  Generates TPC-DS data using DuckDB's `dsdgen`.
2.  Exports tables to `data/tpcds` (Parquet format).
3.  Creates `tpcds.duckdb` with views pointing to the Parquet files.

## 🖥 CLI Usage

The interactive CLI allows you to explore connected data sources, view schemas, and sample data.

**Start the CLI:**
```bash
poetry run data-spectrum
```

**Commands:**

| Command | Usage | Description |
| :--- | :--- | :--- |
| `connect` | `connect <name> <type> [key=val...]` | Connect to a new source (e.g., `duckdb`). |
| `set` | `set <path>` | Switch context to a source or path. |
| `show` | `show [path]` | List databases or objects (Tables/Views). |
| `desc` | `desc <path>` | View schema and full column statistics (Min/Max/Nulls). |
| `sample` | `sample <path> [--limit N] [--method M] [--full]` | Sample data. Use `--full` to disable truncation. |

**Example Session:**
```bash
ds(/)> connect mydb duckdb database=tpcds.duckdb
Successfully connected to mydb

ds(/)> set mydb.main
Context: mydb.main

ds(mydb.main)> show
    Objects in main
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ Name          ┃ Type       ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ customer      │ Table/View │
│ store_sales   │ Table/View │
...

ds(mydb.main)> desc customer
Table: mydb.main.customer
Rows: 1000
    Schema & Statistics
┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ Column           ┃ Type    ┃ Min       ┃ Nulls     ┃
...

ds(mydb.main)> sample customer --limit 5 --full
...
```

## �🚀 Usage Examples

### 1. RDBMS (MySQL / Snowflake / SQLite)
**Scenario**: Connect to a legacy MySQL database.

**Step 1: Register Source**
```bash
curl -X 'POST' \
  'http://localhost:8000/sources' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "legacy_mysql",
  "type": "rdbms",
  "connection_details": {
    "url": "mysql+pymysql://user:pass@localhost:3306/my_db"
  },
  "credentials": {} 
}'
```
*(Note: You can also pass discrete `host`, `port`, `username` fields in `connection_details` and `credentials` supported by the connector).*

**Step 1.5: Discover Databases**
```bash
curl -X 'POST' 'http://localhost:8000/discovery/databases/legacy_mysql'
```

**Step 2: Get Metadata**
```bash
# Get Schema & Stats
curl -X 'GET' 'http://localhost:8000/metadata/users?source_name=legacy_mysql'
```

### 2. Databricks
**Scenario**: Extract metadata from a Unity Catalog table.

**Step 1: Register Source**
```bash
curl -X 'POST' \
  'http://localhost:8000/sources' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "databricks_prod",
  "type": "databricks",
  "connection_details": {
    "host": "adb-123456789.1.azuredatabricks.net",
    "http_path": "/sql/1.0/warehouses/abcdef123"
  },
  "credentials": {
    "token": "dapi123456789..."
  }
}'
```

**Step 1.5: Discover Databases**
```bash
curl -X 'POST' 'http://localhost:8000/discovery/databases/databricks_prod'
```

**Step 2: Get Metadata**
```bash
# Table ID format: catalog.schema.table
curl -X 'GET' 'http://localhost:8000/metadata/main.sales.orders?source_name=databricks_prod'
```

### 3. Databricks (Unity Catalog)
**Scenario**: Zero-compute metadata extraction via REST API.

**Step 1: Register Source**
```bash
curl -X 'POST' \
  'http://localhost:8000/sources' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "databricks_uc",
  "type": "databricks",
  "connection_details": {
    "host": "adb-123456789.1.azuredatabricks.net",
    "http_path": "/" 
  },
  "credentials": {
    "token": "dapi123456789..."
  }
}'
```

**Step 1.5: Discover Databases**
```bash
curl -X 'POST' 'http://localhost:8000/discovery/databases/databricks_uc'
```

**Step 2: Get Metadata**
```bash
# Uses databricks-sdk to fetch schema without waking warehouse
curl -X 'GET' 'http://localhost:8000/metadata/main.sales.orders?source_name=databricks_uc'
```

### 4. Snowflake (Dedicated)
**Scenario**: Extract metadata using native connector.

**Step 1: Register Source**
```bash
curl -X 'POST' \
  'http://localhost:8000/sources' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "snowflake_dw",
  "type": "rdbms",
  "connection_details": {
    "driver": "snowflake",
    "account": "abc123456",
    "warehouse": "COMPUTE_WH",
    "database": "SALES_DB",
    "schema": "PUBLIC"
  },
  "credentials": {
    "username": "user",
    "password": "password"
  }
}'
```

**Step 1.5: Discover Databases**
```bash
curl -X 'POST' 'http://localhost:8000/discovery/databases/snowflake_dw'
```

### 5. AWS S3 (Parquet Files)
**Scenario**: Analyze a bucket of Parquet files partitioned by date.

**Step 1: Register Source**
```bash
curl -X 'POST' \
  'http://localhost:8000/sources' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "s3_datalake",
  "type": "file_system",
  "connection_details": {
    "path": "s3://my-corp-bucket/data/raw",
    "url": "s3"
  },
  "credentials": {
    "aws_access_key_id": "AKIA...",
    "aws_secret_access_key": "SECRET..."
  }
}'
```

**Step 2: Get Metadata**
```bash
# Recursively scans s3://my-corp-bucket/data/raw/transactions
curl -X 'GET' 'http://localhost:8000/metadata/transactions?source_name=s3_datalake'
```

### 6. DuckDB (Embedded)
**Scenario**: Analyze a local DuckDB database (or Parquet files via DuckDB).

**Step 1: Register Source**
```bash
curl -X 'POST' \
  'http://localhost:8000/sources' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "local_duckdb",
  "type": "duckdb",
  "connection_details": {
    "database": "data/tpcds.duckdb" 
  }
}'
```

**Step 1.5: Discover Databases**
```bash
curl -X 'POST' 'http://localhost:8000/discovery/databases/local_duckdb'
```

**Step 2: Discover Tables**
```bash
curl -X 'POST' 'http://localhost:8000/discovery/tables/local_duckdb/main'
```

**Step 3: Get Metadata & Stats**
```bash
# Set include_column_stats=true to get Min/Max/Nulls/Distinct
curl -X 'GET' \
  'http://localhost:8000/metadata/main.customer?source_name=local_duckdb&include_column_stats=true'
```

**Step 4: Sample Data**
```bash
# Sample 10 rows using BERNOULLI method (10%)
curl -X 'POST' \
  'http://localhost:8000/sample/local_duckdb/main.customer?limit=10&method=bernoulli&percent=10'
```

## 🛠 Development

**Prerequisites**
- **Python**: version 3.10 or higher.
- **Poetry**: for dependency management.

**Installation**
```bash
poetry install
```

**Run API**
```bash
poetry run uvicorn src.api.server:app --reload
```
Access Swagger UI at `http://localhost:8000/docs`

**Docker**
Start the API and persistence layer:
```bash
docker-compose up --build
```

**Docker CLI & TPC-DS**
To run the TPC-DS generator and use the CLI with the generated data:
```bash
# Option A: Run everything (API + Generator)
docker-compose --profile tools up --build

# Option B: Run generator one-off
docker-compose run --profile tools generator

# Connect inside CLI (Note: DB name changed in docker-compose)
ds(/)> connect mylake duckdb database=data/sample_tpcds.tiny.duckdb
```

