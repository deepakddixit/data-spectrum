import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.minio import MinioContainer
import sqlalchemy
import boto3
from botocore.client import Config

@pytest.fixture(scope="module")
def postgres_container():
    """Spin up a Postgres container."""
    postgres = PostgresContainer("postgres:15-alpine")
    postgres.start()
    
    # Create a test table
    engine = sqlalchemy.create_engine(postgres.get_connection_url())
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("CREATE TABLE test_users (id SERIAL PRIMARY KEY, name TEXT, age INT)"))
        conn.execute(sqlalchemy.text("INSERT INTO test_users (name, age) VALUES ('Alice', 30), ('Bob', 25)"))
        conn.execute(sqlalchemy.text("INSERT INTO test_users (name, age) VALUES ('Charlie', NULL)"))
    
    yield postgres
    
    postgres.stop()

@pytest.fixture(scope="module")
def minio_container():
    """Spin up a MinIO container."""
    # Explicitly set credentials
    access_key = "minioadmin"
    secret_key = "minioadmin"
    
    minio = MinioContainer("minio/minio:latest")
    minio.with_env("MINIO_ROOT_USER", access_key)
    minio.with_env("MINIO_ROOT_PASSWORD", secret_key)
    minio.start()
    
    # Setup client to upload sample data
    # MinioContainer usually exposes port 9000
    config = Config(signature_version='s3v4')
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{minio.get_container_host_ip()}:{minio.get_exposed_port(9000)}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=config,
        region_name="us-east-1"
    )
    
    # Create bucket and upload file
    bucket_name = "test-bucket"
    client.create_bucket(Bucket=bucket_name)
    
    # We need a sample parquet file. 
    # For now, let's just upload a dummy text file to verify connectivity/listing
    # Or generate a small parquet if pyarrow is avail (it is)
    import pyarrow as pa
    import pyarrow.parquet as pq
    import io
    
    table = pa.Table.from_pydict({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    
    client.put_object(Bucket=bucket_name, Key="data/file1.parquet", Body=buf.getvalue())
    
    yield minio
    
    minio.stop()

@pytest.fixture(scope="module")
def test_client():
    from fastapi.testclient import TestClient
    from src.api.server import app
    return TestClient(app)
