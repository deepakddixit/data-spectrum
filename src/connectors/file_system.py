from typing import List, Optional, Dict, Any
from src.core.models import SamplingMethod
from src.core.interface import DataSource
from src.core.models import (
    DataSourceConfig,
    TableSchema,
    TableStats,
    ColumnStat,
    PartitionInfo
)
from src.connectors.storage import Storage
from src.connectors.file_formats import ParquetReader
from src.connectors.table_formats import DeltaReader

class FileConnector(DataSource):
    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
        self.storage = Storage(config)
        self.parquet_reader = ParquetReader(self.storage)
        self.delta_reader = DeltaReader(config.credentials) # Pass correct storage options

    def connect(self):
        pass # Storage init handled in __init__

    def close(self):
        pass

    def discover_databases(self) -> List[str]:
        # Treat top-level directories as "databases"
        base_path = self.config.connection_details.get("path")
        return self.storage.list_directories(base_path)

    def discover_tables(self, database: str) -> List[str]:
        # Treat sub-directories as "tables"
        # Logic: Look for directories that contain data files or metadata folders
        # This is simplified recursive crawling
        base_path = self.config.connection_details.get("path")
        db_path = f"{base_path.rstrip('/')}/{database}"
        return self.storage.list_directories(db_path)

    def _identify_format(self, table_path: str):
        # check for _delta_log
        files = self.storage.list_files(table_path)
        if any("_delta_log" in f for f in files):
            return "delta"
        if any(f.endswith(".parquet") for f in files):
            return "parquet"
        return "unknown"

    def get_table_schema(self, table_id: str) -> TableSchema:
        # table_id is relative path, or name
        base_path = self.config.connection_details.get("path")
        full_path = f"{base_path.rstrip('/')}/{table_id}"
        
        fmt = self._identify_format(full_path)
        if fmt == "delta":
            schema, _ = self.delta_reader.get_metadata(full_path)
            return schema
        elif fmt == "parquet":
            files = [f for f in self.storage.list_files(full_path, recursive=True) if f.endswith(".parquet")]
            return self.parquet_reader.get_schema(files)
        
        raise ValueError(f"Unknown format for table {table_id}")

    def get_table_stats(self, table_id: str) -> TableStats:
        base_path = self.config.connection_details.get("path")
        full_path = f"{base_path.rstrip('/')}/{table_id}"
        
        fmt = self._identify_format(full_path)
        if fmt == "delta":
             _, stats = self.delta_reader.get_metadata(full_path)
             return stats
        elif fmt == "parquet":
            files = [f for f in self.storage.list_files(full_path, recursive=True) if f.endswith(".parquet")]
            return self.parquet_reader.get_stats(files)
            
        return TableStats(row_count=0)

    def get_column_stats(self, table_id: str, column_name: str) -> ColumnStat:
        # Re-use get_table_stats logic as it computes all cols for parquet usually
        stats = self.get_table_stats(table_id)
        return stats.column_stats.get(column_name)

    def get_partitions(self, table_id: str) -> List[PartitionInfo]:
        return []

    def sample_data(self, table_id: str, limit: Optional[int] = None, method: SamplingMethod = SamplingMethod.LIMIT, percent: Optional[float] = None) -> List[Dict[str, Any]]:
        # Basic implementation for file systems
        # For now, just raise NotImplemented or return empty to fix instantiation check on test
        # In future we can implement actual parquet/delta sampling
        return []
