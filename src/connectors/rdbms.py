from typing import List, Optional, Any, Dict
import sqlalchemy
from sqlalchemy import create_engine, inspect, text, func, select
from src.core.interface import DataSource
from src.core.models import (
    DataSourceConfig,
    TableSchema,
    TableStats,
    ColumnStat,
    Column,
    DataType,
    PartitionInfo,
    SamplingMethod,
    DatabaseObject,
    ObjectType
)

class RDBMSConnector(DataSource):
    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
        self.engine = None
        self.connection = None
        
    def connect(self):
        url = self.config.connection_details.get("url")
        if not url:
            # Construct URL from parts if needed
            driver = self.config.connection_details.get("driver", "sqlite")
            host = self.config.connection_details.get("host", "")
            port = self.config.connection_details.get("port", "")
            database = self.config.connection_details.get("database", "")
            username = self.config.credentials.get("username", "")
            password = self.config.credentials.get("password", "")
            
            if driver == "sqlite":
                 if not database:
                     raise ValueError("SQLite requires 'database' parameter.")
                 url = f"sqlite:///{database}"
            else:
                 missing = []
                 if not host: missing.append("host")
                 if not port: missing.append("port")
                 if not database: missing.append("database")
                 if not username: missing.append("username")
                 # Password might be optional for some configurations, but usually required
                 
                 if missing:
                     raise ValueError(f"Missing required connection parameters for {driver}: {', '.join(missing)}")
                     
                 url = f"{driver}://{username}:{password}@{host}:{port}/{database}"

        self.engine = create_engine(url)
        self.connection = self.engine.connect()

    def close(self):
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()
    
    def discover_databases(self) -> List[str]:
        if not self.engine:
            self.connect()
        inspector = inspect(self.engine)
        return inspector.get_schema_names()

    def discover_tables(self, database: str) -> List[Any]: # List[DatabaseObject]
        if not self.engine:
            self.connect()
        inspector = inspect(self.engine)
        tables = inspector.get_table_names(schema=database)
        views = inspector.get_view_names(schema=database)
        
        objects = []
        for t in tables:
            objects.append(DatabaseObject(name=t, type=ObjectType.TABLE))
        for v in views:
            objects.append(DatabaseObject(name=v, type=ObjectType.VIEW))
            
        return sorted(objects, key=lambda x: x.name)

    def get_table_schema(self, table_id: str) -> TableSchema:
        if not self.engine:
            self.connect()
        
        # Parse table_id "schema.table" or "table"
        parts = table_id.split(".")
        if len(parts) == 2:
            schema, table_name = parts
        else:
            schema, table_name = None, parts[0]

        inspector = inspect(self.engine)
        columns_info = inspector.get_columns(table_name, schema=schema)
        pk_info = inspector.get_pk_constraint(table_name, schema=schema)
        
        columns = []
        for col in columns_info:
            # Map SQLAlchemy types to our DataType enum
            # Simplified mapping
            type_str = str(col['type']).lower()
            if "int" in type_str:
                dt = DataType.INTEGER
            elif "char" in type_str or "text" in type_str:
                dt = DataType.STRING
            elif "float" in type_str or "numeric" in type_str:
                dt = DataType.FLOAT
            elif "bool" in type_str:
                dt = DataType.BOOLEAN
            elif "date" in type_str or "time" in type_str:
                dt = DataType.TIMESTAMP
            else:
                dt = DataType.UNKNOWN

            columns.append(Column(
                name=col['name'],
                data_type=dt,
                nullable=col['nullable'],
                metadata={"original_type": str(col['type'])}
            ))
            
        return TableSchema(
            columns=columns,
            primary_keys=pk_info.get('constrained_columns', [])
        )

    def get_table_stats(self, table_id: str) -> TableStats:
        if not self.engine:
            self.connect()
            
        parts = table_id.split(".")
        if len(parts) == 2:
            schema, table_name = parts
            table_obj = sqlalchemy.table(table_name, schema=schema)
        else:
             table_obj = sqlalchemy.table(parts[0])

        # Get Row Count
        query = select(func.count()).select_from(table_obj)
        row_count = self.connection.execute(query).scalar()
        
        # Basic implementation only gets row count. 
        # Detailed column stats are expensive and should be fetched via get_column_stats or batch
        
        return TableStats(row_count=row_count)

    def get_column_stats(self, table_id: str, column_name: str) -> ColumnStat:
        if not self.engine:
            self.connect()
            
        parts = table_id.split(".")
        if len(parts) == 2:
            schema, table_name = parts
            table_obj = sqlalchemy.table(table_name, schema=schema)
        else:
            table_obj = sqlalchemy.table(parts[0])
            
        col = sqlalchemy.column(column_name)
        
        # Construct query: MIN, MAX, COUNT(NULL), COUNT(DISTINCT)
        # Note: COUNT(DISTINCT) can be expensive on large tables.
        query = select(
            func.min(col),
            func.max(col),
            func.count(col), # Count non-null
            func.count(sqlalchemy.distinct(col))
        ).select_from(table_obj)
        
        result = self.connection.execute(query).fetchone()
        min_val, max_val, count, distinct = result
        
        # Calculate nulls: Total Rows - Count(col)
        # Need total rows first (cached or re-query)
        # For simplicity, just querying count(*) again or we pass it
        total_rows = self.connection.execute(select(func.count()).select_from(table_obj)).scalar()
        null_count = total_rows - count
        
        return ColumnStat(
            column_name=column_name,
            min_value=min_val,
            max_value=max_val,
            null_count=null_count,
            distinct_count=distinct
        )

    def get_partitions(self, table_id: str) -> List[PartitionInfo]:
        # Generic RDBMS usually doesn't expose partitions easily via standard SQL inspection
        return []

    def sample_data(self, table_id: str, limit: Optional[int] = None, method: SamplingMethod = SamplingMethod.LIMIT, percent: Optional[float] = None) -> List[Dict[str, Any]]:
        """Sample data from the table using specified method."""
        if not self.engine:
            self.connect()

        parts = table_id.split(".")
        if len(parts) == 2:
            schema, table_name = parts
            table_obj = sqlalchemy.table(table_name, schema=schema)
        else:
             table_obj = sqlalchemy.table(parts[0])
        
        # We need to select columns explicitly or inspect ensures we can map results 
        # But 'select *' equivalent with sqlalchemy table object defaults to all columns 
        # if wrapped in select()
        
        if method == SamplingMethod.BERNOULLI or method == SamplingMethod.SYSTEM:
            if percent is None:
                raise ValueError("Percent must be specified for BERNOULLI or SYSTEM sampling")
                
            # Use SQLAlchemy tablesample
            # Note: Not all dialects support tablesample.
            
            ts = table_obj.tablesample(percent, name=table_obj.name, method=func.bernoulli if method == SamplingMethod.BERNOULLI else func.system)
            query = select(text("*")).select_from(ts)
            
            # If limit is also provided, we can apply it on top of sample
            if limit is not None:
                query = query.limit(limit)
                
        else:
             # Default LIMIT
             actual_limit = limit if limit is not None else 100
             query = select(text("*")).select_from(table_obj).limit(actual_limit)
        
        # Execute
        result = self.connection.execute(query)
        keys = result.keys()
        
        rows = []
        for row in result:
             # Convert row to dict
             rows.append(dict(zip(keys, row)))
             
        return rows
