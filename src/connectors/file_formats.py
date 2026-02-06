import pyarrow.parquet as pq
from typing import List, Dict, Any
from src.core.models import TableSchema, TableStats, ColumnStat, Column, DataType
from src.connectors.storage import Storage

class ParquetReader:
    def __init__(self, storage: Storage):
        self.storage = storage

    def get_schema(self, files: List[str]) -> TableSchema:
        if not files:
            return TableSchema(columns=[])
        
        # Read schema from the first file
        # fsspec file object
        with self.storage.open(files[0]) as f:
            pq_file = pq.ParquetFile(f)
            schema = pq_file.schema_arrow
            
            columns = []
            for field in schema:
                # Map Arrow types to DataType
                # Simplified
                type_str = str(field.type)
                if "int" in type_str: dt = DataType.INTEGER
                elif "string" in type_str or "utf8" in type_str: dt = DataType.STRING
                elif "float" in type_str: dt = DataType.FLOAT
                else: dt = DataType.UNKNOWN
                
                columns.append(Column(name=field.name, data_type=dt, nullable=field.nullable))
                
            return TableSchema(columns=columns)

    def get_stats(self, files: List[str]) -> TableStats:
        # Aggregation Logic
        total_rows = 0
        total_size = 0
        
        # Column accumulators
        # col_name -> {min, max, null_count, distinct (approx)}
        col_stats_acc = {}

        for file_path in files:
             with self.storage.open(file_path) as f:
                pq_file = pq.ParquetFile(f)
                meta = pq_file.metadata
                total_rows += meta.num_rows
                # total_size += f.size # fsspec file object might not have size attribute directly if stream
                
                # Iterate row groups
                for rg_idx in range(meta.num_row_groups):
                    rg = meta.row_group(rg_idx)
                    for col_idx in range(rg.num_columns):
                        col = rg.column(col_idx)
                        path_in_schema = col.path_in_schema
                        
                        if path_in_schema not in col_stats_acc:
                            col_stats_acc[path_in_schema] = {
                                'min': None, 'max': None, 'null_count': 0
                            }
                        
                        acc = col_stats_acc[path_in_schema]
                        stats = col.statistics
                        if stats:
                            # Update Min
                            if stats.has_min_max:
                                if acc['min'] is None or stats.min < acc['min']:
                                    acc['min'] = stats.min
                                if acc['max'] is None or stats.max > acc['max']:
                                    acc['max'] = stats.max
                            
                            # Update Nulls
                            acc['null_count'] += stats.null_count
        
        # Convert acc to TableStats
        final_col_stats = {}
        for name, acc in col_stats_acc.items():
            final_col_stats[name] = ColumnStat(
                column_name=name,
                min_value=acc['min'],
                max_value=acc['max'],
                null_count=acc['null_count']
            )

        return TableStats(
            row_count=total_rows,
            column_stats=final_col_stats
        )
