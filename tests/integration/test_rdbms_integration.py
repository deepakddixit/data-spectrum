import pytest
from src.connectors.rdbms import RDBMSConnector
from src.core.models import DataSourceConfig, SourceType, DataType

def test_rdbms_postgres_flow(postgres_container):
    # Setup Config
    db_url = postgres_container.get_connection_url()
    # url format: postgresql+psycopg2://user:password@host:port/dbname
    
    config = DataSourceConfig(
        name="test_postgres",
        type=SourceType.RDBMS,
        connection_details={"url": db_url},
        credentials={}
    )
    
    connector = RDBMSConnector(config)
    
    # 1. Test Discovery
    tables = connector.discover_tables("public")
    table_names = [t.name if hasattr(t, 'name') else t for t in tables]
    assert "test_users" in table_names
    
    # 2. Test Schema
    schema = connector.get_table_schema("public.test_users")
    col_names = [c.name for c in schema.columns]
    assert "name" in col_names
    assert "age" in col_names
    
    # Verify types (approximate mapping check)
    age_col = next(c for c in schema.columns if c.name == "age")
    assert age_col.data_type == DataType.INTEGER

    # 3. Test Stats (Row Count)
    stats = connector.get_table_stats("public.test_users")
    assert stats.row_count == 3
    
    # 4. Test Column Stats
    col_stats = connector.get_column_stats("public.test_users", "age")
    assert col_stats.min_value == 25
    assert col_stats.max_value == 30
    assert col_stats.null_count == 1
    assert col_stats.distinct_count == 2
