import pytest
from src.connectors.file_system import FileConnector
from src.core.models import DataSourceConfig, SourceType

def test_s3_minio_flow(minio_container):
    # MinIO Setup details from fixture
    # We need to constructing the URL manually or use what's provided
    endpoint = f"http://{minio_container.get_container_host_ip()}:{minio_container.get_exposed_port(9000)}"
    # fsspec s3fs needs "s3://" path and "client_kwargs" for endpoint_url if not standard AWS
    
    config = DataSourceConfig(
        name="test_s3",
        type=SourceType.FILE_SYSTEM,
        connection_details={
            "url": "s3",
            "path": "s3://test-bucket/data"
        },
        credentials={
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": "minioadmin",
            "endpoint_url": endpoint,
            "use_ssl": "False", # fsspec interprets string 'False' as True? No, better pass boolean if possible, but creds usually string.
            # Actually DataSourceConfig credentials is Dict[str, str] usually? 
            # If our model allows Any, we pass bool.
            # Let's assume Any for credentials or we cast in Storage.
            "config_kwargs": {"s3": {"addressing_style": "path"}} # keys might need mapping in Storage
        }
    )
    # Patch config to pass boolean/dict directly (if model allows)
    config.credentials["use_ssl"] = False
    config.credentials["s3_additional_kwargs"] = {"addressing_style": "path"}
    config.credentials["config_kwargs"] = {"signature_version": "s3v4"}
    config.credentials["region_name"] = "us-east-1"
    
    # Note: Our `Storage` class in `src/connectors/storage.py` maps `aws_access_key_id` etc.
    # It might NOT be mapping `endpoint_url` yet. 
    # We should update `Storage` or pass it in a way s3fs accepts.
    # s3fs accepts `client_kwargs={'endpoint_url': ...}`.
    # Let's hope our Storage class handles extra credentials or we modify it.
    # Checking `src/connectors/storage.py`... 
    # It maps keys to storage_options. We might need to pass endpoint there.
    # For now, let's write the test assuming we might need to fix Storage code if it fails.
    
    # Update credentials with endpoint from container
    config.credentials["endpoint_url"] = endpoint
    
    connector = FileConnector(config)
    
    # Ensure s3fs can list it
    files = connector.storage.list_files("s3://test-bucket/data", recursive=True)
    
    # Assert with debug info
    assert len(files) > 0, "Files list empty"
    assert any("file1.parquet" in f for f in files)
    
    # Get Stats
    stats = connector.get_table_stats("file1.parquet")
    assert stats.row_count == 3
    
    # Get Column Stats
    col_stats = connector.get_column_stats("file1.parquet", "col1")
    assert col_stats.min_value == 1
    assert col_stats.max_value == 3
    assert col_stats.null_count == 0
