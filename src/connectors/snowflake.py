from typing import List, Any
import snowflake.connector
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

class SnowflakeConnector(DataSource):
    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
        self.conn = None
        
    def connect(self):
        # Native connection
        self.conn = snowflake.connector.connect(
            user=self.config.credentials.get("username"),
            password=self.config.credentials.get("password"),
            account=self.config.connection_details.get("account"),
            warehouse=self.config.connection_details.get("warehouse"),
            database=self.config.connection_details.get("database"),
            schema=self.config.connection_details.get("schema")
        )

    def close(self):
        if self.conn:
            self.conn.close()

    def discover_databases(self) -> List[str]:
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        try:
            cursor.execute("SHOW DATABASES")
            return [row[1] for row in cursor.fetchall()] # row[1] is 'name' in Snowflake stats
        finally:
            cursor.close()

    def discover_tables(self, database: str) -> List[str]:
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        try:
            # Assumes database is set or searchable. 
            # If database arg is "schema", list tables there.
            # If "db.schema", parse it.
            cursor.execute(f"SHOW TABLES IN SCHEMA {database}")
            return [row[1] for row in cursor.fetchall()] # row[1] is name
        finally:
            cursor.close()

    def get_table_schema(self, table_id: str) -> TableSchema:
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        try:
            # DESCRIBE TABLE table_id
            cursor.execute(f"DESCRIBE TABLE {table_id}")
            rows = cursor.fetchall()
            
            columns = []
            for row in rows:
                # name, type, kind, null?, default, primary key, unique key, check, expression, comment, policy name
                name = row[0]
                typ = row[1].lower()
                nullable = (row[3] == 'Y')
                
                if "int" in typ or "number" in typ: dt = DataType.INTEGER
                elif "varchar" in typ or "text" in typ or "string" in typ: dt = DataType.STRING
                elif "float" in typ or "double" in typ: dt = DataType.FLOAT
                elif "boolean" in typ: dt = DataType.BOOLEAN
                elif "date" in typ or "time" in typ: dt = DataType.TIMESTAMP
                else: dt = DataType.UNKNOWN
                
                columns.append(Column(name=name, data_type=dt, nullable=nullable))
            
            return TableSchema(columns=columns)
        finally:
            cursor.close()

    def get_table_stats(self, table_id: str) -> TableStats:
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        try:
            # Specialized Metric: TABLE_STORAGE_METRICS
            # Or just SHOW TABLES LIKE 'name' gives rows, bytes
            parts = table_id.split(".")
            table_name = parts[-1]
            
            # Use efficiently cached system query if possible. 
            # SHOW TABLES is fast.
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            row = cursor.fetchone()
            # columns: created_on, name, database_name, schema_name, kind, comment, cluster_by, rows, bytes, owner, retention_time, automatic_clustering, change_tracking, search_optimization, search_optimization_progress, search_optimization_bytes, is_external
            
            if row:
                row_count = row[7]
                size_bytes = row[8]
                return TableStats(row_count=row_count, total_size_bytes=size_bytes)
            
            return TableStats(row_count=0)
        finally:
            cursor.close()

    def get_column_stats(self, table_id: str, column_name: str) -> ColumnStat:
        if not self.conn:
            self.connect()
            
        cursor = self.conn.cursor()
        try:
            # 1. OPTIMIZATION: Check INFORMATION_SCHEMA? 
            # Snowflake Info Schema/Account Usage does NOT typically hold min/max/distinct for columns.
            # But the user suggested checking system tables. 
            # We implemented a robust query using APPROX_COUNT_DISTINCT which is metadata-optimized (HyperLogLog).
            # True exact MIN/MAX still requires a scan, but we can sample if latency is an issue.
            # For now, we perform a standard query but optimized for unique counts.
            
            # Use APPROX_COUNT_DISTINCT for large datasets
            query = f"""
                SELECT 
                    MIN({column_name}), 
                    MAX({column_name}), 
                    COUNT({column_name}), 
                    APPROX_COUNT_DISTINCT({column_name}) 
                FROM {table_id}
            """
            cursor.execute(query)
            res = cursor.fetchone()
            
            # Get Total
            cursor.execute(f"SELECT COUNT(*) FROM {table_id}")
            total = cursor.fetchone()[0]
            
            return ColumnStat(
                column_name=column_name,
                min_value=res[0],
                max_value=res[1],
                null_count=total - res[2],
                distinct_count=res[3]
            )
        finally:
            cursor.close()

    def get_partitions(self, table_id: str) -> List[PartitionInfo]:
        # Snowflake Micro-partitions are hidden.
        # But we can get Clustering Info if clustered.
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT SYSTEM$CLUSTERING_INFORMATION('{table_id}')")
            res = cursor.fetchone()
            # Returns JSON string with depth, overlap etc.
            # Parsing this into PartitionInfo is approximate as they aren't physical partitions in Hive sense.
            return []
        except Exception:
            return []
        finally:
            cursor.close()
