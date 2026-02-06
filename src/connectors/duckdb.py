from typing import List, Any, Dict, Optional
from sqlalchemy import create_engine
from src.core.models import DataSourceConfig, TableSchema, Column, DataType, SamplingMethod, DatabaseObject, ObjectType
from src.connectors.rdbms import RDBMSConnector

class DuckDBConnector(RDBMSConnector):
    def connect(self):
        """
        Connects to a DuckDB database.
        Supports in-memory ('memory') or file-based usage.
        """
        # DuckDB uses no host/port/user/password usually, just a file path or nothing for in-memory
        database = self.config.connection_details.get("database")
        
        if not database:
             raise ValueError("DuckDB connection requires 'database' parameter. Use ':memory:' for in-memory database.")
        
        if database == "memory" or database == ":memory:":
            url = "duckdb:///:memory:"
        else:
            # Assumes local file path
            url = f"duckdb:///{database}"
            
        self.engine = create_engine(url)
        self.connection = self.engine.connect()
        
        # DuckDB specific initialization if needed
        # e.g. self.connection.execute("INSTALL httpfs; LOAD httpfs;") if we strictly need it
        # for s3/remote files, but sticking to basic for now.

    def discover_databases(self) -> List[str]:
        # DuckDB usually has a single catalog, maybe schemas?
        # Default behavior of RDBMSConnector might work if inspector supports get_schema_names
        # But for duckdb, often people just care about tables in 'main' or other schemas
        return super().discover_databases()

    
    def discover_tables(self, database: str) -> List[Any]:
        if not self.engine:
            self.connect()
            
        # DuckDB generic discovery via SQL is often reliable
        # inspector.get_table_names() might work for tables, but we want views too.
        from sqlalchemy import inspect
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
            
        # table_id might be "main.users" or "users"
        # PRAGMA table_info works with 'schema.table' string literal or just table
        
        # Use native DuckDB pragma
        # We need to sanitize input or allow sqlalchemy to handle execution, but PRAGMA usually takes string
        
        # Safer to use parameterized query if possible, but PRAGMA syntax in duckdb is specific.
        # "PRAGMA table_info('table_name')"
        
        # We'll use text()
        from sqlalchemy import text
        from src.core.models import TableSchema, Column, DataType
        
        query = text(f"PRAGMA table_info('{table_id}')")
        result = self.connection.execute(query)
        
        columns = []
        pks = []
        
        # cid, name, type, notnull, dflt_value, pk
        for row in result:
            col_name = row[1]
            col_type = row[2].upper()
            notnull = row[3]
            is_pk = row[5]
            
            # Map types
            if "INT" in col_type:
                dt = DataType.INTEGER
            elif "VARCHAR" in col_type or "STRING" in col_type or "TEXT" in col_type:
                dt = DataType.STRING
            elif "FLOAT" in col_type or "DOUBLE" in col_type or "DECIMAL" in col_type:
                dt = DataType.FLOAT
            elif "BOOL" in col_type:
                dt = DataType.BOOLEAN
            elif "TIMESTAMP" in col_type or "DATE" in col_type:
                dt = DataType.TIMESTAMP
            else:
                dt = DataType.UNKNOWN
                
            columns.append(Column(
                name=col_name,
                data_type=dt,
                nullable=not notnull,
                metadata={"original_type": col_type}
            ))
            
            if is_pk:
                pks.append(col_name)
                
        return TableSchema(columns=columns, primary_keys=pks)

    def sample_data(self, table_id: str, limit: Optional[int] = None, method: SamplingMethod = SamplingMethod.LIMIT, percent: Optional[float] = None) -> List[Dict[str, Any]]:
        """Sample data from the table using DuckDB specific sampling."""
        if not self.engine:
            self.connect()

        # Sanitize table_id slightly to avoid obvious injection if possible, though we trust internal call
        # but better to use identifiers. 
        # But we need to construct the FROM clause manually for USING SAMPLE
        
        # Syntax: SELECT * FROM table_name USING SAMPLE percent% (method) LIMIT limit
        
        from sqlalchemy import text
        
        if method == SamplingMethod.BERNOULLI or method == SamplingMethod.SYSTEM:
            if percent is None:
                raise ValueError("Percent must be specified for BERNOULLI or SYSTEM sampling")
            
            # Construct raw SQL query because SQLAlchemy's tablesample might not generate 'USING SAMPLE' for duckdb dialect
            # and duckdb-engine might not support generic tablesample compilation yet.
            
            # Determine method string for SQL
            # DuckDB uses: USING SAMPLE 10% (bernoulli) or just (bernoulli)
            method_str = f"({method.value})"
            
            # We assume table_id is safe enough or we should quote it.
            # Simple quoting for now
            safe_table = f'"{table_id}"' if '.' not in table_id else table_id # If dot, assumes schema.table, potentially needs splitting and quoting parts
            
            # Split and quote parts if needed
            if '.' in table_id:
                parts = table_id.split('.')
                safe_table = '.'.join([f'"{p}"' for p in parts])
            else:
                safe_table = f'"{table_id}"'
            
            query_str = f"SELECT * FROM {safe_table} USING SAMPLE {percent}% {method_str}"
            
            if limit is not None:
                query_str += f" LIMIT {limit}"
                
            query = text(query_str)
            
        else:
             return super().sample_data(table_id, limit, method, percent)
        
        # Execute
        result = self.connection.execute(query)
        keys = result.keys()
        
        rows = []
        for row in result:
             rows.append(dict(zip(keys, row)))
             
        return rows
