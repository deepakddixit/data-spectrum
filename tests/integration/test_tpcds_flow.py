import pytest
import os
import shutil
import subprocess
from pathlib import Path
import time

DATA_DIR = Path("data/test_tpcds")
DB_PATH = Path("data/test_tpcds.duckdb")

@pytest.fixture(scope="module")
def tpcds_setup():
    """Generate TPC-DS data and database, then cleanup."""
    print("\nGenerating TPC-DS data (SF=0.01)...")
    
    # Ensure clean state
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    if DB_PATH.exists():
        os.remove(DB_PATH)
        
    start_time = time.time()
    # Call the generator script
    # We use subprocess to isolate the generation environment
    cmd = [
        "python", "utils/generate_tpcds.py",
        "--sf", "0.01",
        "--out", str(DATA_DIR),
        "--db", str(DB_PATH)
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        pytest.fail(f"TPC-DS Generation failed: {result.stderr}")
        
    print(f"Generation completed in {time.time() - start_time:.2f}s")
    
    yield str(DB_PATH)
    
    # Cleanup
    print("\nCleaning up TPC-DS data...")
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    if DB_PATH.exists():
        os.remove(DB_PATH)

def test_tpcds_e2e_flow(test_client, tpcds_setup):
    """Run end-to-end flow against generated TPC-DS data."""
    db_path = tpcds_setup
    source_name = "test_tpcds"
    
    # 1. Register Source
    print(f"Connecting to {db_path}...")
    resp = test_client.post("/sources", json={
        "name": source_name,
        "type": "duckdb",
        "connection_details": {"database": db_path}
    })
    assert resp.status_code == 200, f"Register failed: {resp.text}"
    
    # 2. Discover Databases
    print("Discovering databases...")
    resp = test_client.post(f"/discovery/databases/{source_name}")
    assert resp.status_code == 200
    dbs = resp.json()
    # DuckDB usually has 'main' or 'memory' or the attached db name
    # Using 'main' is standard for the default schema in the file
    pass 
    
    # 3. Discover Tables
    print("Discovering tables...")
    resp = test_client.post(f"/discovery/tables/{source_name}/main")
    assert resp.status_code == 200
    tables = resp.json()
    # tables is list of DatabaseObject or strings depending on API version (we kept it compatible)
    # The API returns what `store.get_discovery` returns which is JSON.
    # Check for core tables
    table_names = [t.get('name') if isinstance(t, dict) else t for t in tables]
    assert "customer" in table_names
    assert "store_sales" in table_names
    
    # 4. Get Metadata (Customer)
    print("Fetching metadata for 'customer'...")
    resp = test_client.get(f"/metadata/main.customer?source_name={source_name}&include_column_stats=true")
    assert resp.status_code == 200
    metadata = resp.json()
    
    # Verify basics
    assert metadata['name'] == "customer"
    assert metadata['stats']['row_count'] > 0
    
    # Verify Column Stats
    col_stats = metadata['stats']['column_stats']
    # 'c_customer_sk' should be present
    assert 'c_customer_sk' in col_stats
    
    # 5. Sample Data
    print("Sampling 'customer' data...")
    # Using LIMIT method
    resp = test_client.post(f"/sample/{source_name}/main.customer?limit=5")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 5
    assert 'c_customer_id' in data[0]

    # Using BERNOULLI method
    resp = test_client.post(f"/sample/{source_name}/main.customer?method=bernoulli&percent=10")
    assert resp.status_code == 200
    # Can't guarantee exact count with percent, but should be list
    assert isinstance(resp.json(), list)

    print("TPC-DS End-to-End Test Passed!")
