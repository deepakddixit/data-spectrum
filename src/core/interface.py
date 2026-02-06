from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from src.core.models import (
    DataSourceConfig,
    TableSchema,
    TableStats,
    ColumnStat,
    PartitionInfo,
    TableMetadata,
    SamplingMethod,
    DatabaseObject
)

class DataSource(ABC):
    def __init__(self, config: DataSourceConfig):
        self.config = config

    @abstractmethod
    def connect(self):
        """Establish connection to the data source."""
        pass

    @abstractmethod
    def close(self):
        """Close connection to the data source."""
        pass
        
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def discover_databases(self) -> List[str]:
        """List available databases or top-level containers."""
        pass

    @abstractmethod
    def discover_tables(self, database: str) -> List[DatabaseObject]:
        """List tables within a specific database/container."""
        pass

    @abstractmethod
    def get_table_schema(self, table_id: str) -> TableSchema:
        """Get schema for a specific table."""
        pass

    @abstractmethod
    def get_table_stats(self, table_id: str) -> TableStats:
        """Get table-level statistics (row count, size, etc.)."""
        pass

    @abstractmethod
    def get_column_stats(self, table_id: str, column_name: str) -> ColumnStat:
        """Get statistics for a specific column."""
        pass

    @abstractmethod
    def get_partitions(self, table_id: str) -> List[PartitionInfo]:
        """Get partition information for a table."""
        pass

    @abstractmethod
    def sample_data(self, table_id: str, limit: Optional[int] = None, method: SamplingMethod = SamplingMethod.LIMIT, percent: Optional[float] = None) -> List[Dict[str, Any]]:
        """Sample data from the table."""
        pass

    def extract_full_metadata(self, table_id: str) -> TableMetadata:
        """Helper to extract full metadata by combining granular calls."""
        # Note: This is a default implementation, subclasses can override for optimization
        # Parsing table_id to get name and database might be connector specific
        # ideally table_id should be "db.table" or just path depending on source
        
        schema = self.get_table_schema(table_id)
        stats = self.get_table_stats(table_id)
        partitions = self.get_partitions(table_id)
        
        # Simplified parsing, assumes table_id format "db.table" or "table"
        parts = table_id.split(".")
        if len(parts) >= 2:
            database = parts[0]
            name = ".".join(parts[1:])
        else:
            database = "default"
            name = table_id

        return TableMetadata(
            id=table_id,
            name=name,
            database=database,
            schema_info=schema,
            stats=stats,
            partitions=partitions
        )
