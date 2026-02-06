from typing import List, Optional, Any
from databricks.sdk import WorkspaceClient
from src.core.interface import DataSource
from src.core.models import (
    DataSourceConfig,
    TableSchema,
    TableStats,
    ColumnStat,
    Column,
    DataType,
    PartitionInfo,
    SourceType
)
from src.connectors.file_system import FileConnector

class DatabricksUCConnector(DataSource):
    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
        self.client = None
        self._file_connector_cache = {} # Cache connectors for paths
        
    def connect(self):
        self.client = WorkspaceClient(
            host=self.config.connection_details.get("host"),
            token=self.config.credentials.get("token")
        )

    def close(self):
        pass

    def discover_databases(self) -> List[str]:
        if not self.client:
            self.connect()
        catalogs = self.client.catalogs.list()
        return [c.name for c in catalogs]

    def discover_tables(self, database: str) -> List[str]:
        if not self.client:
            self.connect()
            
        parts = database.split(".")
        if len(parts) == 1:
            catalog = parts[0]
            try:
                schemas = self.client.schemas.list(catalog_name=catalog)
                tables = []
                for s in schemas:
                    try:
                        t_list = self.client.tables.list(catalog_name=catalog, schema_name=s.name)
                        tables.extend([f"{s.name}.{t.name}" for t in t_list])
                    except Exception:
                        continue
                return tables
            except Exception:
                return []
        else:
            catalog, schema = parts[0], parts[1]
            try:
                tables = self.client.tables.list(catalog_name=catalog, schema_name=schema)
                return [t.name for t in tables]
            except Exception:
                return []

    def get_table_schema(self, table_id: str) -> TableSchema:
        if not self.client:
            self.connect()
        
        table = self.client.tables.get(full_name=table_id)
        columns = []
        for col in table.columns:
            type_text = col.type_text.lower()
            if "int" in type_text or "long" in type_text: dt = DataType.INTEGER
            elif "string" in type_text: dt = DataType.STRING
            elif "double" in type_text or "float" in type_text: dt = DataType.FLOAT
            elif "timestamp" in type_text: dt = DataType.TIMESTAMP
            elif "boolean" in type_text: dt = DataType.BOOLEAN
            else: dt = DataType.UNKNOWN
            
            columns.append(Column(
                name=col.name,
                data_type=dt,
                nullable=col.type_nullable,
                metadata={"comment": col.comment}
            ))
            
        return TableSchema(columns=columns)

    def _get_file_connector(self, storage_location: str) -> Optional[FileConnector]:
        if not storage_location:
            return None
            
        # Determine protocol
        if storage_location.startswith("s3"):
             proto = "s3"
        elif storage_location.startswith("abfss"):
             proto = "abfss"
        elif storage_location.startswith("gs"):
             proto = "gs"
        else:
             proto = "file" # Fallback/Local?

        # Create nested config
        # Reuse existing credentials (user must provide cloud keys in the main config)
        cfg = DataSourceConfig(
            name=f"nested_{proto}",
            type=SourceType.FILE_SYSTEM,
            connection_details={"path": storage_location, "url": proto},
            credentials=self.config.credentials # Pass through AWS/Azure keys
        )
        return FileConnector(cfg)

    def get_table_stats(self, table_id: str) -> TableStats:
        if not self.client:
            self.connect()
            
        table = self.client.tables.get(full_name=table_id)
        
        # 1. Check Table Properties
        props = table.properties or {}
        if 'numRows' in props:
             # Some systems inject this
             return TableStats(row_count=int(props['numRows']))
             
        # Check Delta specific
        if 'delta.logRetentionDuration' in props:
            # It's delta, might have advanced properties or we rely on FileConnector
            pass

        # 2. File Connector Fallback
        if table.storage_location:
            fc = self._get_file_connector(table.storage_location)
            if fc:
                try:
                    # FileConnector expects relative path usually from its root, 
                    # but here we configured root = storage_location.
                    # So we pass "" or "." as table_id? 
                    # FileConnector logic uses `path/{table_id}`.
                    # We should probably configure FileConnector with parent and pass leaf?
                    # Or simpler: FileConnector.get_table_stats(".") if path is full.
                    
                    # Hack: pass empty string as ID since root is the table location
                    return fc.get_table_stats("") 
                except Exception as e:
                    print(f"Fallback to file stats failed: {e}")
        
        return TableStats(row_count=0)

    def get_column_stats(self, table_id: str, column_name: str) -> ColumnStat:
        if not self.client:
            self.connect()
            
        table = self.client.tables.get(full_name=table_id)
        
        # 1. Check Properties? (Unlikely for cols)
        
        # 2. File Connector Fallback
        if table.storage_location:
            fc = self._get_file_connector(table.storage_location)
            if fc:
                try:
                    return fc.get_column_stats("", column_name)
                except Exception:
                    pass

        return ColumnStat(column_name=column_name)

    def get_partitions(self, table_id: str) -> List[PartitionInfo]:
        if not self.client:
            self.connect()
            
        table = self.client.tables.get(full_name=table_id)
        
        if table.storage_location:
            fc = self._get_file_connector(table.storage_location)
            if fc:
                try:
                    return fc.get_partitions("")
                except Exception:
                    pass
                    
        return []
