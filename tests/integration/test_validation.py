import pytest
from src.core.models import DataSourceConfig, SourceType
from src.connectors.duckdb import DuckDBConnector
from src.connectors.rdbms import RDBMSConnector

def test_duckdb_missing_database():
    """Verify DuckDB connector raises ValueError if database is missing."""
    config = DataSourceConfig(
        name="test_bad_duck",
        type=SourceType.DUCKDB,
        connection_details={}
    )
    connector = DuckDBConnector(config)
    
    with pytest.raises(ValueError, match="DuckDB connection requires 'database' parameter"):
        connector.connect()

def test_rdbms_sqlite_missing_database():
    """Verify RDBMS (sqlite) raises ValueError if database is missing."""
    config = DataSourceConfig(
        name="test_bad_sqlite",
        type=SourceType.RDBMS,
        connection_details={"driver": "sqlite"}
    )
    connector = RDBMSConnector(config)
    
    with pytest.raises(ValueError, match="SQLite requires 'database' parameter"):
        connector.connect()

def test_rdbms_postgres_missing_params():
    """Verify RDBMS (generic) raises ValueError if required params are missing."""
    config = DataSourceConfig(
        name="test_bad_pg",
        type=SourceType.RDBMS,
        connection_details={"driver": "postgresql"}
        # Missing host, port, database, etc.
    )
    connector = RDBMSConnector(config)
    
    with pytest.raises(ValueError, match="Missing required connection parameters"):
        connector.connect()
