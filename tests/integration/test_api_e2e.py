import pytest
from fastapi.testclient import TestClient
from src.api.server import app

client = TestClient(app)

def test_e2e_rdbms_flow(postgres_container):
    db_url = postgres_container.get_connection_url()
    
    # 1. Register Source
    response = client.post("/sources", json={
        "name": "e2e_postgres",
        "type": "rdbms",
        "connection_details": {"url": db_url},
        "credentials": {}
    })
    assert response.status_code == 200
    assert response.json()["name"] == "e2e_postgres"
    
    # 2. List Sources (Verify Persistence/Registry)
    response = client.get("/sources")
    assert response.status_code == 200
    sources = response.json()
    assert any(s["name"] == "e2e_postgres" for s in sources)
    
    # 3. Discovery (Not fully implemented in routes.py usually, but we check if endpoint exists/returns 501 or mock)
    # Our routes.py currently returns "Not implemented yet" for discovery
    response = client.post("/discovery/databases/e2e_postgres")
    assert response.status_code == 200 # It returns simple dict currently
    
    # 4. Metadata Extraction (The real deal)
    # Get metadata for the table 'test_users' we created in fixture
    # Table ID in postgres is usually 'test_users' or 'public.test_users'
    # RDBMS connector uses `discover_tables` returning simple names depending on implementation.
    # Let's try 'public.test_users' as RDBMSConnector.get_table_schema expects schema.table or table
    
    response = client.get("/metadata/public.test_users?source_name=e2e_postgres")
    assert response.status_code == 200, f"Metadata extraction failed: {response.text}"
    
    metadata = response.json()
    assert metadata["name"] == "test_users"
    assert metadata["schema_info"]["columns"][0]["name"] == "id"
    # Check stats
    assert metadata["stats"]["row_count"] == 3
