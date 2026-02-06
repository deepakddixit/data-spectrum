from typing import Any
from src.core.models import TableSchema, TableStats, ColumnStat, Column, DataType
try:
    from deltalake import DeltaTable
except ImportError:
    DeltaTable = None

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.table import Table as IcebergTable
except ImportError:
    IcebergTable = None

class DeltaReader:
    def __init__(self, storage_options: dict = None):
        self.storage_options = storage_options or {}

    def is_available(self):
        return DeltaTable is not None

    def get_metadata(self, path: str) -> (TableSchema, TableStats):
        if not self.is_available():
            raise ImportError("deltalake library not installed")
        
        dt = DeltaTable(path, storage_options=self.storage_options)
        
        # Schema
        schema_arrow = dt.schema().to_pyarrow()
        columns = []
        for field in schema_arrow:
            type_str = str(field.type)
            if "int" in type_str: dt_type = DataType.INTEGER
            elif "string" in type_str: dt_type = DataType.STRING
            elif "float" in type_str: dt_type = DataType.FLOAT
            else: dt_type = DataType.UNKNOWN
            columns.append(Column(name=field.name, data_type=dt_type, nullable=field.nullable))
            
        schema = TableSchema(columns=columns)
        
        # Stats
        # DeltaTable.metadata() gives protocol, etc.
        # dt.files() gives list of files.
        # To get aggregated stats, we might need add_actions iteration or verify if dt exposes summary.
        # dt.get_add_actions(flatten=True) returns a nice Arrow table with stats.
        
        # Simplified for now: just get row count if available easily, else skip or compute
        # Rust binding usually fast.
        # For now return placeholder stats or minimal
        stats = TableStats(row_count=0) # functionality varies by version
        
        return schema, stats

class IcebergReader:
    def __init__(self, catalog_config: dict = None):
        # Iceberg usually needs a catalog_config
        self.catalog_config = catalog_config or {}

    def is_available(self):
        return IcebergTable is not None

    def get_metadata(self, table_identifier: str) -> (TableSchema, TableStats):
        # Only works if we have a catalog setup. 
        # For filesystem table support (HadoopCatalog or similar), pyiceberg support varies.
        pass
