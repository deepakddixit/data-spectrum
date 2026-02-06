from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field

class DataType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    BINARY = "binary"
    STRUCT = "struct"
    ARRAY = "array"
    MAP = "map"
    UNKNOWN = "unknown"

class Column(BaseModel):
    name: str
    data_type: DataType
    nullable: bool = True
    metadata: Dict[str, Any] = Field(default_factory=dict)

class TableSchema(BaseModel):
    columns: List[Column]
    primary_keys: List[str] = Field(default_factory=list)

class ColumnStat(BaseModel):
    column_name: str
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    null_count: Optional[int] = None
    distinct_count: Optional[int] = None
    avg_len: Optional[float] = None
    max_len: Optional[int] = None

class TableStats(BaseModel):
    row_count: int
    total_size_bytes: Optional[int] = None
    last_modified: Optional[datetime] = None
    column_stats: Dict[str, ColumnStat] = Field(default_factory=dict)

class PartitionInfo(BaseModel):
    column_name: str
    value: Any
    location: str
    file_count: Optional[int] = None
    size_bytes: Optional[int] = None

class TableMetadata(BaseModel):
    id: str
    name: str
    database: str
    schema_info: TableSchema
    stats: Optional[TableStats] = None
    partitions: List[PartitionInfo] = Field(default_factory=list)

class SourceType(str, Enum):
    RDBMS = "rdbms"
    DATABRICKS = "databricks"
    FILE_SYSTEM = "file_system"
    DUCKDB = "duckdb"

class ObjectType(str, Enum):
    TABLE = "table"
    VIEW = "view"
    UNKNOWN = "unknown"

class DatabaseObject(BaseModel):
    name: str
    type: ObjectType = ObjectType.UNKNOWN


class SamplingMethod(str, Enum):
    LIMIT = "limit"
    BERNOULLI = "bernoulli"
    SYSTEM = "system"

class DataSourceConfig(BaseModel):
    name: str
    type: SourceType
    connection_details: Dict[str, Any] = Field(
        ...,
        description="Connection parameters specific to the source type.",
        json_schema_extra={
            "examples": [
                {"database": "/path/to/db.duckdb"},  # DuckDB
                {"database": ":memory:"},             # DuckDB Memory
                {"url": "postgresql://user:pass@host:5432/db"}, # RDBMS URL
                {"driver": "postgresql", "host": "localhost", "port": 5432, "database": "db", "username": "user"}, # RDBMS explicit
                {"driver": "sqlite", "database": "/path/to/db.sqlite"}, # SQLite
                {"path": "s3://bucket/data"} # FileSystem
            ]
        }
    )
    credentials: Dict[str, Any] = Field(default_factory=dict) # Stores username, password, tokens (to be encrypted)
    options: Dict[str, Any] = Field(default_factory=dict) # Other options like include/exclude patterns
