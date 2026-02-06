from src.core.models import DataSourceConfig, SourceType
from src.core.interface import DataSource
from src.connectors.rdbms import RDBMSConnector
from src.connectors.file_system import FileConnector
from src.connectors.databricks import DatabricksConnector
from src.connectors.databricks_uc import DatabricksUCConnector
from src.connectors.snowflake import SnowflakeConnector

from src.connectors.duckdb import DuckDBConnector

def get_datasource(config: DataSourceConfig) -> DataSource:
    """Factory to create appropriate DataSource instance."""
    
    if config.type == SourceType.RDBMS:
        driver = config.connection_details.get("driver", "").lower()
        if driver == "snowflake":
            return SnowflakeConnector(config)
        return RDBMSConnector(config)
    
    elif config.type == SourceType.DUCKDB:
        return DuckDBConnector(config)
        
    elif config.type == SourceType.DATABRICKS:
        # Heuristic: Check http_path for warehouse to decide SQL vs UC
        http_path = config.connection_details.get("http_path", "")
        if "warehouses" in http_path:
            return DatabricksConnector(config)
        return DatabricksUCConnector(config)
        
    elif config.type == SourceType.FILE_SYSTEM:
        return FileConnector(config)
        
    raise ValueError(f"Unsupported source type: {config.type}")
