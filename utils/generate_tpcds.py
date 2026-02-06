
import os
import duckdb
import argparse
from pathlib import Path

def generate_tpcds(scale_factor: float, output_dir: str):
    """
    Generates TPC-DS data using DuckDB and exports to Parquet.
    """
    print(f"Initializing DuckDB and generating TPC-DS data (SF={scale_factor})...")
    conn = duckdb.connect()
    # Try to load extensions. INSTALL might fail if offline but extension might be pre-baked.
    try:
        conn.execute("INSTALL tpcds;")
        conn.execute("INSTALL httpfs;") 
    except Exception as e:
        print(f"Warning during extension install (might be already installed): {e}")
    
    conn.execute("LOAD tpcds;")
    conn.execute(f"CALL dsdgen(sf={scale_factor});")
    
    # Get list of tables generated
    tables = conn.execute("SHOW TABLES").fetchall()
    tables = [t[0] for t in tables]
    
    print(f"Generated {len(tables)} tables. Exporting to Parquet in {output_dir}...")
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    for table in tables:
        print(f"Exporting {table}...")
        table_dir = output_path / table
        table_dir.mkdir(exist_ok=True)
        # Export as parquet file(s)
        # We can use COPY ... TO ... (FORMAT PARQUET)
        # Partitioning could be added here if needed, but keeping it simple
        conn.execute(f"COPY {table} TO '{table_dir}/{table}.parquet' (FORMAT PARQUET)")
        
    conn.close()
    print("Data generation complete.")

def create_attached_db(parquet_dir: str, db_path: str):
    """
    Creates a DuckDB database that reads the generated Parquet files.
    """
    print(f"Creating DuckDB database at {db_path} linked to {parquet_dir}...")
    
    if os.path.exists(db_path):
        os.remove(db_path)
        
    conn = duckdb.connect(db_path)
    parquet_path = Path(parquet_dir)
    
    # Find all table directories
    for table_dir in parquet_path.iterdir():
        if table_dir.is_dir():
            table_name = table_dir.name
            # Create a view for each table
            # Using glob pattern to read all parquet files in the dir
            query = f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{table_dir}/*.parquet')"
            conn.execute(query)
            print(f"Created view for {table_name}")
            
    conn.close()
    print(f"Database created successfully at {db_path}")

def main():
    parser = argparse.ArgumentParser(description="Generate TPC-DS data in Parquet and create DuckDB wrapper.")
    parser.add_argument("--sf", type=float, default=0.01, help="Scale factor for TPC-DS (default: 0.01)")
    parser.add_argument("--out", type=str, default="data/tpcds", help="Output directory for Parquet files")
    parser.add_argument("--db", type=str, default="tpcds.duckdb", help="Path for the output DuckDB database")
    
    args = parser.parse_args()
    
    generate_tpcds(args.sf, args.out)
    create_attached_db(args.out, args.db)

if __name__ == "__main__":
    main()
