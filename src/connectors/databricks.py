from typing import List, Any
from databricks import sql
from src.core.interface import DataSource
from src.core.models import (
    DataSourceConfig,
    TableSchema,
    TableStats,
    ColumnStat,
    Column,
    DataType,
    PartitionInfo
)

class DatabricksConnector(DataSource):
    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
        self.connection = None
        
    def connect(self):
        self.connection = sql.connect(
            server_hostname=self.config.connection_details.get("host"),
            http_path=self.config.connection_details.get("http_path"),
            access_token=self.config.credentials.get("token")
        )

    def close(self):
        if self.connection:
            self.connection.close()

    def discover_databases(self) -> List[str]:
        if not self.connection:
            self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            return [row.databaseName for row in cursor.fetchall()]

    def discover_tables(self, database: str) -> List[str]:
        if not self.connection:
            self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(f"SHOW TABLES IN {database}")
            return [row.tableName for row in cursor.fetchall()]

    def get_table_schema(self, table_id: str) -> TableSchema:
        if not self.connection:
            self.connect()
        
        # table_id expected as "catalog.schema.table" or "schema.table"
        with self.connection.cursor() as cursor:
            cursor.execute(f"DESCRIBE {table_id}")
            rows = cursor.fetchall()
            
            columns = []
            for row in rows:
                col_name = row.col_name
                if not col_name or col_name.startswith("#"): 
                    continue # Skip partition info headers
                
                type_str = row.data_type.lower()
                # Map Types
                if "int" in type_str or "bigint" in type_str: dt = DataType.INTEGER
                elif "string" in type_str: dt = DataType.STRING
                elif "double" in type_str or "float" in type_str: dt = DataType.FLOAT
                elif "timestamp" in type_str: dt = DataType.TIMESTAMP
                elif "boolean" in type_str: dt = DataType.BOOLEAN
                else: dt = DataType.UNKNOWN
                
                columns.append(Column(name=col_name, data_type=dt))
                
            return TableSchema(columns=columns)

    def get_table_stats(self, table_id: str) -> TableStats:
        if not self.connection:
            self.connect()
            
        with self.connection.cursor() as cursor:
            # Try to get simplified stats from DESCRIBE DETAIL (Delta tables)
            try:
                cursor.execute(f"DESCRIBE DETAIL {table_id}")
                detail = cursor.fetchone()
                # numFiles, sizeInBytes, numRecords are usually available for Delta
                # This depends on rows mapping, assuming typical Delta output
                # detail keys often need inspection, access via index or name if Row object
                # Start with COUNT(*) if generic
                pass
            except Exception:
                pass
            
            cursor.execute(f"SELECT COUNT(*) FROM {table_id}")
            row_count = cursor.fetchone()[0]
            
            return TableStats(row_count=row_count)

    def get_column_stats(self, table_id: str, column_name: str) -> ColumnStat:
        if not self.connection:
            self.connect()
            
        # SQL based stats
        # For advanced stats (file based fallback), we would check if this is a Delta table, 
        # get the 'location' from DESCRIBE DETAIL, and instantiate a FileConnector.
        
        with self.connection.cursor() as cursor:
            query = f"""
                SELECT 
                    min({column_name}), 
                    max({column_name}), 
                    count({column_name}), 
                    count(distinct {column_name}) 
                FROM {table_id}
            """
            cursor.execute(query)
            row = cursor.fetchone()
            
            # Need total count for nulls
            cursor.execute(f"SELECT COUNT(*) FROM {table_id}")
            total = cursor.fetchone()[0]
            
            return ColumnStat(
                column_name=column_name,
                min_value=row[0],
                max_value=row[1],
                null_count=total - row[2],
                distinct_count=row[3]
            )

    def get_partitions(self, table_id: str) -> List[PartitionInfo]:
        if not self.connection:
            self.connect()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"SHOW PARTITIONS {table_id}")
                rows = cursor.fetchall()
                # Parse partition info
                return [] # simplified for now
        except Exception:
            return []
